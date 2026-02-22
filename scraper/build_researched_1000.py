#!/usr/bin/env python3
"""Build 1000 researched+verified entries using web search + HTTP validation."""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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


SOURCES = [
    {"tool": "Slack", "domain": "slack.com", "must_include": ["/customer-stories/"], "queries": ['site:slack.com "customer-stories"']},
    {"tool": "Asana", "domain": "asana.com", "must_include": ["/customers/", "/case-study/"], "queries": ['site:asana.com "customers" "case study"']},
    {"tool": "Miro", "domain": "miro.com", "must_include": ["/customers/"], "queries": ['site:miro.com "customers"']},
    {"tool": "Zoom", "domain": "zoom.com", "must_include": ["/customer-stories/"], "queries": ['site:zoom.com "customer-stories"']},
    {"tool": "Figma", "domain": "figma.com", "must_include": ["/customers/"], "queries": ['site:figma.com "customers"']},
    {"tool": "Notion", "domain": "notion.so", "must_include": ["/customers/"], "queries": ['site:notion.so "customers"']},
    {"tool": "Notion", "domain": "notion.com", "must_include": ["/customers/"], "queries": ['site:notion.com "customers"']},
    {"tool": "Monday.com", "domain": "monday.com", "must_include": ["/customers/"], "queries": ['site:monday.com "customers"']},
    {"tool": "Canva", "domain": "canva.com", "must_include": ["/case-studies/"], "queries": ['site:canva.com "case-studies"']},
    {"tool": "Linear", "domain": "linear.app", "must_include": ["/customers/"], "queries": ['site:linear.app "customers"']},
    {"tool": "Smartsheet", "domain": "smartsheet.com", "must_include": ["/customers/"], "queries": ['site:smartsheet.com "customers"']},
    {"tool": "Airtable", "domain": "airtable.com", "must_include": ["/customers/"], "queries": ['site:airtable.com "customers"']},
    {"tool": "ClickUp", "domain": "clickup.com", "must_include": ["/customers/"], "queries": ['site:clickup.com "customers"']},
    {"tool": "Wrike", "domain": "wrike.com", "must_include": ["/customers/"], "queries": ['site:wrike.com "customers"']},
    {"tool": "Mural", "domain": "mural.co", "must_include": ["/customers/"], "queries": ['site:mural.co "customers"']},
    {"tool": "Dropbox", "domain": "dropbox.com", "must_include": ["/customers/"], "queries": ['site:dropbox.com "customers"']},
    {"tool": "Box", "domain": "box.com", "must_include": ["/customers/"], "queries": ['site:box.com "customers"']},
    {"tool": "Lucid", "domain": "lucid.co", "must_include": ["/customers/"], "queries": ['site:lucid.co "customers"']},
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_brave_key() -> str:
    cfg_path = Path.home() / ".openclaw" / "openclaw.json"
    if cfg_path.exists():
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        return cfg.get("tools", {}).get("web", {}).get("search", {}).get("apiKey", "")
    return ""


def static_reject(url: str) -> bool:
    try:
        p = requests.utils.urlparse(url)
    except Exception:
        return True
    if p.scheme not in ("http", "https"):
        return True
    path = p.path or "/"
    for patt in ROOT_PATTERNS:
        if patt.search(path):
            return True
    return False


def extract_title(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()[:200]


def verify_url(url: str, timeout: int = 8) -> Tuple[bool, Optional[int], str, str]:
    if static_reject(url):
        return False, None, "", ""
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code < 400 and not static_reject(r.url):
            return True, r.status_code, r.url, ""
    except requests.RequestException:
        pass
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        if r.status_code >= 400 or static_reject(r.url):
            return False, r.status_code, r.url, ""
        title = ""
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype:
            chunks = []
            total = 0
            for chunk in r.iter_content(chunk_size=4096):
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total >= 50000:
                    break
            if chunks:
                html = b"".join(chunks).decode(r.encoding or "utf-8", errors="ignore")
                title = extract_title(html)
        return True, r.status_code, r.url, title
    except requests.RequestException:
        return False, None, "", ""


def brave_search(key: str, query: str, count: int, offset: int) -> List[Dict[str, Any]]:
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"Accept": "application/json", "X-Subscription-Token": key},
            params={"q": query, "count": count, "offset": offset},
            timeout=15,
        )
        return r.json().get("web", {}).get("results", [])
    except Exception:
        return []


def company_from_url(url: str, fallback: str) -> str:
    try:
        p = requests.utils.urlparse(url)
    except Exception:
        return fallback[:120] if fallback else "Unknown"
    segs = [s for s in p.path.split("/") if s]
    if not segs:
        return fallback[:120] if fallback else "Unknown"
    slug = segs[-1]
    slug = slug.replace(".html", "").replace(".htm", "")
    slug = re.sub(r"[-_]+", " ", slug).strip()
    if not slug:
        return fallback[:120] if fallback else "Unknown"
    return slug.title()[:120]


def build_companies(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for it in items:
        cname = it["company"]
        if cname not in grouped:
            grouped[cname] = {
                "company": cname,
                "domain": it["source_host"],
                "industry": "Unknown",
                "tools": [],
                "updated_at": datetime.now().date().isoformat(),
            }
        g = grouped[cname]
        if any(t.get("name") == it["tool_name"] for t in g["tools"]):
            continue
        g["tools"].append(
            {
                "name": it["tool_name"],
                "use_case": it["use_case"],
                "source_url": it["source_url"],
                "source_type": "vendor_case_study",
                "verified": True,
                "http_status": it["http_status"],
                "checked_at": it["checked_at"],
                "final_url": it["final_url"],
                "evidence_title": it["evidence_title"],
            }
        )
    return sorted(grouped.values(), key=lambda x: x["company"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=int, default=1000)
    parser.add_argument("--count-per-query", type=int, default=20)
    parser.add_argument("--pages-per-query", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--output-flat", default="data/researched_verified_1000.json")
    parser.add_argument("--output-db", default="data/companies.json")
    args = parser.parse_args()

    key = load_brave_key()
    if not key:
        raise SystemExit("Brave API key not found")

    checked_at = now_iso()
    items: List[Dict[str, Any]] = []
    seen_final_urls: Set[str] = set()
    stats = {"searched_queries": 0, "search_results_seen": 0, "verified_added": 0}

    for src in SOURCES:
        for q in src["queries"]:
            for page in range(args.pages_per_query):
                if len(items) >= args.target:
                    break
                offset = page * args.count_per_query
                stats["searched_queries"] += 1
                results = brave_search(key, q, args.count_per_query, offset)
                if not results:
                    continue
                for r in results:
                    if len(items) >= args.target:
                        break
                    stats["search_results_seen"] += 1
                    url = (r.get("url") or "").strip()
                    title = (r.get("title") or "").strip()
                    if not url:
                        continue
                    lu = url.lower()
                    if src["domain"] not in lu:
                        continue
                    if not any(token in lu for token in src["must_include"]):
                        continue
                    ok, status, final_url, evidence_title = verify_url(url, timeout=args.timeout)
                    if not ok:
                        continue
                    if final_url in seen_final_urls:
                        continue
                    seen_final_urls.add(final_url)
                    company = company_from_url(final_url, title)
                    item = {
                        "company": company,
                        "tool_name": src["tool"],
                        "use_case": f"{company} team collaboration with {src['tool']}",
                        "source_url": final_url,
                        "final_url": final_url,
                        "source_host": requests.utils.urlparse(final_url).hostname or src["domain"],
                        "http_status": status,
                        "checked_at": checked_at,
                        "evidence_title": evidence_title or title,
                        "verified": True,
                        "research_query": q,
                    }
                    items.append(item)
                    stats["verified_added"] += 1
                time.sleep(args.sleep)
            if len(items) >= args.target:
                break
        if len(items) >= args.target:
            break

    out_flat = Path(args.output_flat)
    out_db = Path(args.output_db)
    backup_db = out_db.parent / f"companies.before_research_build.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    if out_db.exists():
        backup_db.write_text(out_db.read_text(encoding="utf-8"), encoding="utf-8")

    flat_payload = {
        "meta": {
            "generated_at": checked_at,
            "target": args.target,
            "actual": len(items),
            "source": "brave_search + http_validation",
            "stats": stats,
        },
        "items": items,
    }
    out_flat.write_text(json.dumps(flat_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    companies = build_companies(items)
    out_db.write_text(json.dumps(companies, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "generated_at": checked_at,
        "target": args.target,
        "actual_items": len(items),
        "actual_companies": len(companies),
        "output_flat": str(out_flat),
        "output_db": str(out_db),
        "backup_db": str(backup_db) if backup_db.exists() else "",
        "stats": stats,
    }
    report_path = out_db.parent / "research_build_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
