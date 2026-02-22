#!/usr/bin/env python3
"""URL reliability cleaner for companies.json.

Rules:
- Remove structurally invalid URLs (root listings, typo domains, fake customer paths)
- Validate remaining URLs by HTTP (HEAD then GET fallback)
- Keep retry queue for transient statuses (403/429)
- Store quarantine records for removed URLs
"""

from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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

BAD_DOMAINS = {
    "community.",
    "support.",
    "help.",
    "forum.",
    "reddit.com",
    "news.ycombinator.com",
    "quora.com",
    "medium.com",
    "linkedin.com",
}

RETRYABLE_STATUSES = {403, 429}


@dataclass
class CheckResult:
    original_url: str
    ok: bool
    status_code: Optional[int]
    final_url: Optional[str]
    reason: str
    retryable: bool = False
    evidence_title: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_url(url: str) -> Optional[requests.models.PreparedRequest]:
    try:
        req = requests.Request("GET", url).prepare()
        return req
    except Exception:
        return None


def static_reject_reason(url: str) -> Optional[str]:
    if not url:
        return "empty_url"

    req = parse_url(url)
    if not req or not req.url.startswith(("http://", "https://")):
        return "invalid_url_format"

    lowered = req.url.lower()
    host = requests.utils.urlparse(req.url).hostname or ""

    if any(token in lowered for token in BAD_DOMAINS):
        return "blocked_domain"

    for pattern in BAD_HOST_PATTERNS:
        if pattern.search(host):
            return "invalid_host_pattern"

    path = requests.utils.urlparse(req.url).path or "/"
    for pattern in ROOT_PATTERNS:
        if pattern.search(path):
            return "root_or_listing_page"

    return None


def extract_title(text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    title = re.sub(r"\s+", " ", m.group(1)).strip()
    return title[:200]


def check_http(url: str, timeout: int = 6) -> CheckResult:
    # HEAD first
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        status = r.status_code
        final_url = r.url

        reject = static_reject_reason(final_url)
        if status < 400 and not reject:
            return CheckResult(url, True, status, final_url, "ok")
        if status in RETRYABLE_STATUSES:
            return CheckResult(url, False, status, final_url, f"http_{status}", retryable=True)
    except Exception:
        status = None
        final_url = None

    # GET fallback for providers that reject HEAD
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        status = r.status_code
        final_url = r.url
        reject = static_reject_reason(final_url)

        title = ""
        content_type = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type:
            try:
                chunks: List[bytes] = []
                total = 0
                for chunk in r.iter_content(chunk_size=4096):
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= 50000:
                        break
                snippet = b"".join(chunks).decode(r.encoding or "utf-8", errors="ignore")
                if snippet:
                    title = extract_title(snippet)
            except Exception:
                title = ""

        if status < 400 and not reject:
            return CheckResult(url, True, status, final_url, "ok", evidence_title=title)
        if status in RETRYABLE_STATUSES:
            return CheckResult(url, False, status, final_url, f"http_{status}", retryable=True)
        if reject:
            return CheckResult(url, False, status, final_url, reject)
        return CheckResult(url, False, status, final_url, f"http_{status}")
    except requests.RequestException:
        return CheckResult(url, False, None, None, "network_error")


def build_index(data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    idx: Dict[str, List[Dict[str, Any]]] = {}
    for company in data:
        for tool in company.get("tools", []):
            url = (tool.get("source_url") or "").strip()
            if not url:
                continue
            idx.setdefault(url, []).append({"company": company, "tool": tool})
    return idx


def clean_data(data_file: Path, workers: int, timeout: int) -> Dict[str, Any]:
    checked_at = now_iso()

    with data_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    url_index = build_index(data)
    unique_urls = list(url_index.keys())

    report: Dict[str, Any] = {
        "checked_at": checked_at,
        "total_companies": len(data),
        "total_unique_urls": len(unique_urls),
        "total_tool_entries": sum(len(c.get("tools", [])) for c in data),
        "cleaned_entries": 0,
        "quarantined_entries": 0,
        "retry_entries": 0,
        "reasons": {},
    }

    quarantine: List[Dict[str, Any]] = []
    retry_queue: List[Dict[str, Any]] = []

    # 1) Static filtering first
    to_check: List[str] = []
    for url in unique_urls:
        reason = static_reject_reason(url)
        if reason:
            for ref in url_index[url]:
                tool = ref["tool"]
                company = ref["company"]
                quarantine.append(
                    {
                        "company": company.get("company", ""),
                        "tool": tool.get("name", ""),
                        "source_url": url,
                        "final_url": "",
                        "http_status": None,
                        "reason": reason,
                        "checked_at": checked_at,
                    }
                )
                tool["source_url"] = ""
                tool["verified"] = False
                tool["source_type"] = "unverified"
                tool["http_status"] = None
                tool["checked_at"] = checked_at
                tool["final_url"] = ""
                tool["evidence_title"] = ""
                report["quarantined_entries"] += 1
                report["reasons"][reason] = report["reasons"].get(reason, 0) + 1
        else:
            to_check.append(url)

    # 2) HTTP verification on static-pass URLs
    results: Dict[str, CheckResult] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {
            pool.submit(check_http, url, timeout): url
            for url in to_check
        }
        for fut in as_completed(future_map):
            url = future_map[fut]
            try:
                results[url] = fut.result()
            except Exception:
                results[url] = CheckResult(url, False, None, None, "checker_crash")

    for url, result in results.items():
        refs = url_index[url]
        for ref in refs:
            tool = ref["tool"]
            company = ref["company"]

            tool["http_status"] = result.status_code
            tool["checked_at"] = checked_at
            tool["final_url"] = result.final_url or ""
            tool["evidence_title"] = result.evidence_title

            if result.ok:
                tool["source_url"] = result.final_url or url
                tool["verified"] = True
                report["cleaned_entries"] += 1
                continue

            tool["verified"] = False
            report["reasons"][result.reason] = report["reasons"].get(result.reason, 0) + 1

            if result.retryable:
                retry_queue.append(
                    {
                        "company": company.get("company", ""),
                        "tool": tool.get("name", ""),
                        "source_url": url,
                        "final_url": result.final_url or "",
                        "http_status": result.status_code,
                        "reason": result.reason,
                        "checked_at": checked_at,
                    }
                )
                report["retry_entries"] += 1
            else:
                quarantine.append(
                    {
                        "company": company.get("company", ""),
                        "tool": tool.get("name", ""),
                        "source_url": url,
                        "final_url": result.final_url or "",
                        "http_status": result.status_code,
                        "reason": result.reason,
                        "checked_at": checked_at,
                    }
                )
                tool["source_url"] = ""
                tool["source_type"] = "unverified"
                report["quarantined_entries"] += 1

    # Save outputs
    data_dir = data_file.parent
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = data_dir / f"companies.json.bak.{timestamp}"
    quarantine_path = data_dir / "quarantine_404.json"
    retry_path = data_dir / "retry_queue.json"
    report_path = data_dir / "url_cleaning_report.json"

    # backup must preserve pre-cleaned data
    with data_file.open("r", encoding="utf-8") as f:
        original_text = f.read()
    backup_path.write_text(original_text, encoding="utf-8")

    data_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    quarantine_path.write_text(json.dumps(quarantine, ensure_ascii=False, indent=2), encoding="utf-8")
    retry_path.write_text(json.dumps(retry_queue, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    report["backup_path"] = str(backup_path)
    report["quarantine_path"] = str(quarantine_path)
    report["retry_path"] = str(retry_path)
    report["report_path"] = str(report_path)
    report["verified_after"] = sum(
        1
        for company in data
        for tool in company.get("tools", [])
        if tool.get("verified")
    )

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean unreliable URLs in companies dataset")
    parser.add_argument("--data", default="data/companies.json", help="Path to companies.json")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers for HTTP checks")
    parser.add_argument("--timeout", type=int, default=8, help="HTTP timeout seconds")
    args = parser.parse_args()

    data_file = Path(args.data)
    if not data_file.exists():
        raise SystemExit(f"Data file not found: {data_file}")

    report = clean_data(data_file, workers=args.workers, timeout=args.timeout)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
