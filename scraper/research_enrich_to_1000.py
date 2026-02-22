#!/usr/bin/env python3
"""Research-enrich companies.json to reach target verified tool entries."""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}

ROOT_PATTERNS = [
    re.compile(r"/customers/?$", re.IGNORECASE),
    re.compile(r"/customer-stories/?$", re.IGNORECASE),
    re.compile(r"/customers/search", re.IGNORECASE),
    re.compile(r"/en/customer-stories/?$", re.IGNORECASE),
    re.compile(r"/intl/[^/]+/customer-stories/?$", re.IGNORECASE),
    re.compile(r"/case-studies/?$", re.IGNORECASE),
    re.compile(r"/success-stories\.html$", re.IGNORECASE),
]

BAD_HOST_PATTERNS = [
    re.compile(r"(^|\.)monday\.com\.com$", re.IGNORECASE),
    re.compile(r"(^|\.)microsoftteams\.com$", re.IGNORECASE),
    re.compile(r"(^|\.)jira\.com$", re.IGNORECASE),
    re.compile(r"(^|\.)confluence\.com$", re.IGNORECASE),
]

PREFERRED_DOMAINS = {
    "Slack": ["slack.com"],
    "Microsoft Teams": ["microsoft.com", "customers.microsoft.com", "news.microsoft.com"],
    "Zoom": ["zoom.com"],
    "Jira": ["atlassian.com"],
    "Confluence": ["atlassian.com"],
    "Notion": ["notion.so", "notion.com"],
    "Figma": ["figma.com"],
    "Miro": ["miro.com"],
    "Monday.com": ["monday.com"],
    "Asana": ["asana.com"],
    "Canva": ["canva.com"],
    "Linear": ["linear.app"],
    "Mural": ["mural.co"],
    "Smartsheet": ["smartsheet.com"],
    "Airtable": ["airtable.com"],
}


@dataclass
class UrlCheck:
    ok: bool
    status: Optional[int]
    final_url: str
    title: str
    reason: str


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def static_reject(url: str) -> Optional[str]:
    if not url:
        return "empty_url"
    try:
        u = requests.utils.urlparse(url)
    except Exception:
        return "invalid_url"
    if u.scheme not in ("http", "https"):
        return "invalid_scheme"
    host = (u.hostname or "").lower()
    for p in BAD_HOST_PATTERNS:
        if p.search(host):
            return "invalid_host_pattern"
    path = u.path or "/"
    for p in ROOT_PATTERNS:
        if p.search(path):
            return "root_or_listing_page"
    return None


def extract_title(text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()[:200]


def check_url(url: str, timeout: int = 8) -> UrlCheck:
    rejected = static_reject(url)
    if rejected:
        return UrlCheck(False, None, "", "", rejected)

    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code < 400 and not static_reject(r.url):
            return UrlCheck(True, r.status_code, r.url, "", "ok")
    except requests.RequestException:
        pass

    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        status = r.status_code
        final = r.url
        if status >= 400:
            return UrlCheck(False, status, final, "", f"http_{status}")
        rej = static_reject(final)
        if rej:
            return UrlCheck(False, status, final, "", rej)
        title = ""
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype:
            chunks: List[bytes] = []
            total = 0
            for ch in r.iter_content(chunk_size=4096):
                if not ch:
                    break
                chunks.append(ch)
                total += len(ch)
                if total >= 50000:
                    break
            if chunks:
                html = b"".join(chunks).decode(r.encoding or "utf-8", errors="ignore")
                title = extract_title(html)
        return UrlCheck(True, status, final, title, "ok")
    except requests.RequestException:
        return UrlCheck(False, None, "", "", "network_error")


def load_brave_key() -> str:
    cfg_path = Path.home() / ".openclaw" / "openclaw.json"
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            key = cfg.get("tools", {}).get("web", {}).get("search", {}).get("apiKey", "")
            if key:
                return key
        except Exception:
            pass
    return ""


def brave_search(api_key: str, query: str, count: int = 6) -> List[Dict[str, str]]:
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"Accept": "application/json", "X-Subscription-Token": api_key},
            params={"q": query, "count": count},
            timeout=12,
        )
        data = r.json()
        return [{"url": x.get("url", ""), "title": x.get("title", "")} for x in data.get("web", {}).get("results", [])]
    except Exception:
        return []


def total_verified(data: List[Dict[str, Any]]) -> int:
    return sum(1 for c in data for t in c.get("tools", []) if t.get("verified") is True)


def ensure_company(data_map: Dict[str, Dict[str, Any]], template: Dict[str, Any]) -> Dict[str, Any]:
    name = template.get("company", "")
    if name not in data_map:
        data_map[name] = {
            "company": name,
            "domain": template.get("domain", ""),
            "industry": template.get("industry", "Unknown"),
            "tools": [],
            "updated_at": datetime.now().date().isoformat(),
        }
    return data_map[name]


def has_pair(data_map: Dict[str, Dict[str, Any]], company: str, tool: str) -> bool:
    c = data_map.get(company)
    if not c:
        return False
    return any(t.get("name") == tool for t in c.get("tools", []))


def add_verified_entry(
    data_map: Dict[str, Dict[str, Any]],
    template_company: Dict[str, Any],
    template_tool: Dict[str, Any],
    checked: UrlCheck,
    source_url: str,
    checked_at: str,
) -> None:
    company = ensure_company(data_map, template_company)
    tool_name = template_tool.get("name", "")
    tools = company.setdefault("tools", [])
    if any(t.get("name") == tool_name for t in tools):
        return
    tools.append(
        {
            "name": tool_name,
            "use_case": template_tool.get("use_case", ""),
            "source_url": checked.final_url or source_url,
            "source_type": template_tool.get("source_type", "vendor_case_study") or "vendor_case_study",
            "verified": True,
            "http_status": checked.status,
            "checked_at": checked_at,
            "final_url": checked.final_url or source_url,
            "evidence_title": checked.title,
        }
    )
    company["updated_at"] = datetime.now().date().isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Research enrich to target verified entry count")
    parser.add_argument("--data", default="data/companies.json")
    parser.add_argument("--backup", default="data/companies.json.bak.20260221_132018")
    parser.add_argument("--target", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--timeout", type=int, default=8)
    args = parser.parse_args()

    data_path = Path(args.data)
    backup_path = Path(args.backup)
    if not data_path.exists() or not backup_path.exists():
        raise SystemExit("Missing data or backup file")

    checked_at = now_iso()
    data = json.loads(data_path.read_text(encoding="utf-8"))
    source = json.loads(backup_path.read_text(encoding="utf-8"))
    data_map: Dict[str, Dict[str, Any]] = {c.get("company", ""): c for c in data if c.get("company")}

    start_verified = total_verified(list(data_map.values()))

    # Candidate pool from backup
    candidates: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for c in source:
        cname = c.get("company", "")
        if not cname:
            continue
        for t in c.get("tools", []):
            tname = t.get("name", "")
            if not tname:
                continue
            if has_pair(data_map, cname, tname):
                continue
            candidates.append((c, t))

    # Phase 1: validate original URLs from backup
    url_cache: Dict[str, UrlCheck] = {}
    phase1_added = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for c, t in candidates:
            u = (t.get("source_url") or "").strip()
            if not u:
                continue
            if u in url_cache:
                continue
            futures[pool.submit(check_url, u, args.timeout)] = u
        for fut in as_completed(futures):
            u = futures[fut]
            try:
                url_cache[u] = fut.result()
            except Exception:
                url_cache[u] = UrlCheck(False, None, "", "", "checker_error")

    for c, t in candidates:
        if total_verified(list(data_map.values())) >= args.target:
            break
        u = (t.get("source_url") or "").strip()
        if not u:
            continue
        checked = url_cache.get(u)
        if checked and checked.ok:
            if not has_pair(data_map, c.get("company", ""), t.get("name", "")):
                add_verified_entry(data_map, c, t, checked, u, checked_at)
                phase1_added += 1

    # Phase 2: Brave search for unresolved pairs
    api_key = load_brave_key()
    phase2_added = 0
    search_attempts = 0
    if total_verified(list(data_map.values())) < args.target and api_key:
        unresolved: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for c, t in candidates:
            if not has_pair(data_map, c.get("company", ""), t.get("name", "")):
                unresolved.append((c, t))

        for c, t in unresolved:
            if total_verified(list(data_map.values())) >= args.target:
                break
            cname = c.get("company", "")
            tname = t.get("name", "")
            domains = PREFERRED_DOMAINS.get(tname, [])
            queries = [
                f'"{cname}" "{tname}" customer story',
                f'"{cname}" "{tname}" case study',
                f'"{cname}" uses "{tname}"',
            ]
            found: Optional[Tuple[str, UrlCheck]] = None
            for q in queries:
                search_attempts += 1
                results = brave_search(api_key, q, count=6)
                # Prefer tool-vendor domain hits first
                results.sort(
                    key=lambda x: 0
                    if any(d in (x.get("url", "").lower()) for d in domains)
                    else 1
                )
                for r in results:
                    url = (r.get("url") or "").strip()
                    if not url:
                        continue
                    checked = check_url(url, args.timeout)
                    if checked.ok:
                        found = (url, checked)
                        break
                if found:
                    break
                time.sleep(0.25)

            if found and not has_pair(data_map, cname, tname):
                add_verified_entry(data_map, c, t, found[1], found[0], checked_at)
                phase2_added += 1

    final_data = sorted(data_map.values(), key=lambda x: x.get("company", ""))
    final_verified = total_verified(final_data)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    before_backup = data_path.parent / f"companies.before_research_enrich.{ts}.json"
    before_backup.write_text(data_path.read_text(encoding="utf-8"), encoding="utf-8")
    data_path.write_text(json.dumps(final_data, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "checked_at": checked_at,
        "target": args.target,
        "start_verified": start_verified,
        "final_verified": final_verified,
        "phase1_added": phase1_added,
        "phase2_added": phase2_added,
        "search_attempts": search_attempts,
        "used_backup": str(backup_path),
        "before_backup": str(before_backup),
        "data_path": str(data_path),
        "target_reached": final_verified >= args.target,
    }
    report_path = data_path.parent / "research_enrich_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
