#!/usr/bin/env python3
"""Build 1000 verified entries from vendor sitemap customer-story URLs."""

from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree as ET

import requests

HEADERS = {"User-Agent": "Mozilla/5.0"}
NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

SOURCES = [
    {
        "tool": "Slack",
        "sitemap": "https://slack.com/sitemap.xml",
        "regex": r"^https://slack\.com/(?:intl/[a-z]{2}(?:-[a-z]{2})?/)?customer-stories/[^/?#]+/?$",
        "max_sitemaps": 300,
    },
    {
        "tool": "Notion",
        "sitemap": "https://www.notion.com/sitemap.xml",
        "regex": r"^https://www\.notion\.com/[a-z]{2}(?:-[a-z]{2})?/customers/[^/?#]+/?$|^https://www\.notion\.com/customers/[^/?#]+/?$",
        "max_sitemaps": 320,
    },
    {
        "tool": "ClickUp",
        "sitemap": "https://clickup.com/sitemap.xml",
        "regex": r"^https://clickup\.com/customers/[^/?#]+/?$",
        "max_sitemaps": 320,
    },
    {
        "tool": "Airtable",
        "sitemap": "https://www.airtable.com/sitemap.xml",
        "regex": r"^https://www\.airtable\.com/customer-stories/[^/?#]+/?$",
        "max_sitemaps": 80,
    },
    {
        "tool": "Asana",
        "sitemap": "https://asana.com/sitemap.xml",
        "regex": r"^https://asana\.com/case-study/[^/?#]+/?$",
        "max_sitemaps": 80,
    },
    {
        "tool": "Monday.com",
        "sitemap": "https://monday.com/sitemap.xml",
        "regex": r"^https://monday\.com/(?:lang/[a-z]{2}(?:-[a-z]{2})?/)?customers/[^/?#]+/?$",
        "max_sitemaps": 80,
    },
    {
        "tool": "Smartsheet",
        "sitemap": "https://www.smartsheet.com/sitemap.xml",
        "regex": r"^https://www\.smartsheet\.com/customers/[^/?#]+/?$",
        "max_sitemaps": 80,
    },
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_xml(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.text


def parse_sitemap(xml_text: str) -> Tuple[List[str], List[str]]:
    root = ET.fromstring(xml_text)
    urls = [e.text.strip() for e in root.findall(".//sm:url/sm:loc", NS) if e.text]
    maps = [e.text.strip() for e in root.findall(".//sm:sitemap/sm:loc", NS) if e.text]
    return urls, maps


def crawl_candidates(source: Dict[str, Any], cap: int) -> List[Tuple[str, str]]:
    patt = re.compile(source["regex"], re.IGNORECASE)
    queue = [source["sitemap"]]
    seen_maps: Set[str] = set()
    out: List[Tuple[str, str]] = []
    seen_urls: Set[str] = set()

    while queue and len(seen_maps) < source["max_sitemaps"] and len(out) < cap:
        sm = queue.pop(0)
        if sm in seen_maps:
            continue
        seen_maps.add(sm)
        try:
            xml = fetch_xml(sm)
            urls, maps = parse_sitemap(xml)
        except Exception:
            continue
        queue.extend(maps)
        for u in urls:
            if u in seen_urls:
                continue
            if patt.search(u):
                seen_urls.add(u)
                out.append((source["tool"], u))
                if len(out) >= cap:
                    break
    return out


def extract_title(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()[:200]


def verify_url(url: str, timeout: int = 8) -> Tuple[bool, Optional[int], str, str]:
    try:
        h = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if h.status_code < 400:
            return True, h.status_code, h.url, ""
    except requests.RequestException:
        pass
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        if r.status_code >= 400:
            return False, r.status_code, r.url, ""
        title = ""
        if "text/html" in (r.headers.get("Content-Type") or "").lower():
            chunks: List[bytes] = []
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


def company_from_url(url: str) -> str:
    p = requests.utils.urlparse(url)
    segs = [s for s in p.path.split("/") if s]
    if not segs:
        return "Unknown"
    slug = segs[-1].replace(".html", "").replace(".htm", "")
    return re.sub(r"[-_]+", " ", slug).strip().title()[:120] or "Unknown"


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
        if any(t.get("name") == it["tool_name"] for t in grouped[cname]["tools"]):
            continue
        grouped[cname]["tools"].append(
            {
                "name": it["tool_name"],
                "use_case": f"{cname} team collaboration with {it['tool_name']}",
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
    parser.add_argument("--candidate-cap", type=int, default=6000)
    parser.add_argument("--workers", type=int, default=40)
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--output-flat", default="data/researched_verified_1000.json")
    parser.add_argument("--output-db", default="data/companies.researched1000.json")
    args = parser.parse_args()

    checked_at = now_iso()

    candidates: List[Tuple[str, str]] = []
    for s in SOURCES:
        need = max(800, args.target)
        candidates.extend(crawl_candidates(s, cap=need))
        if len(candidates) >= args.candidate_cap:
            break
    # Dedup candidate URLs
    dedup = {}
    for tool, url in candidates:
        dedup[url] = tool
    candidates = [(tool, url) for url, tool in dedup.items()]

    verified_items: List[Dict[str, Any]] = []
    seen_final: Set[str] = set()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_map = {pool.submit(verify_url, url, args.timeout): (tool, url) for tool, url in candidates}
        for fut in as_completed(future_map):
            tool, source_url = future_map[fut]
            ok, status, final_url, title = fut.result()
            if not ok or not final_url:
                continue
            if final_url in seen_final:
                continue
            seen_final.add(final_url)
            host = requests.utils.urlparse(final_url).hostname or ""
            if final_url.rstrip("/").endswith("/customers/list-all"):
                continue
            company = company_from_url(final_url)
            verified_items.append(
                {
                    "company": company,
                    "tool_name": tool,
                    "source_url": final_url,
                    "final_url": final_url,
                    "source_host": host,
                    "http_status": status,
                    "checked_at": checked_at,
                    "evidence_title": title,
                    "verified": True,
                }
            )
            if len(verified_items) >= args.target:
                break

    verified_items = verified_items[: args.target]

    out_flat = Path(args.output_flat)
    out_db = Path(args.output_db)
    flat_payload = {
        "meta": {
            "generated_at": checked_at,
            "target": args.target,
            "actual": len(verified_items),
            "candidates": len(candidates),
        },
        "items": verified_items,
    }
    out_flat.write_text(json.dumps(flat_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    companies = build_companies(verified_items)
    out_db.write_text(json.dumps(companies, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "generated_at": checked_at,
        "target": args.target,
        "actual_items": len(verified_items),
        "actual_companies": len(companies),
        "candidates": len(candidates),
        "output_flat": str(out_flat),
        "output_db": str(out_db),
    }
    Path("data/research_build_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
