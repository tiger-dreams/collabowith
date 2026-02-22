#!/usr/bin/env python3
"""Retry pending URL validations and restore recovered entries into companies.json."""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def static_reject_reason(url: str) -> Optional[str]:
    if not url:
        return "empty_url"
    try:
        parsed = requests.utils.urlparse(url)
    except Exception:
        return "invalid_url_format"
    if parsed.scheme not in ("http", "https"):
        return "invalid_url_format"
    path = parsed.path or "/"
    for pattern in ROOT_PATTERNS:
        if pattern.search(path):
            return "root_or_listing_page"
    return None


def extract_title(text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()[:200]


def http_check_once(url: str, timeout: int) -> Tuple[bool, Optional[int], str, str]:
    """Returns (ok, status, final_url, evidence_title)."""
    # HEAD pass
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        status = r.status_code
        final_url = r.url
        if status < 400 and not static_reject_reason(final_url):
            return True, status, final_url, ""
    except requests.RequestException:
        pass

    # GET fallback
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        status = r.status_code
        final_url = r.url
        title = ""
        if status < 400 and not static_reject_reason(final_url):
            content_type = (r.headers.get("Content-Type") or "").lower()
            if "text/html" in content_type:
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
            return True, status, final_url, title
        return False, status, final_url, ""
    except requests.RequestException:
        return False, None, "", ""


def retry_check(url: str, attempts: int, timeout: int, base_sleep: float) -> Tuple[bool, Optional[int], str, str]:
    last_status: Optional[int] = None
    last_final = ""
    last_title = ""
    for i in range(attempts):
        ok, status, final_url, title = http_check_once(url, timeout)
        if ok:
            return ok, status, final_url, title
        last_status = status
        last_final = final_url
        last_title = title
        if i < attempts - 1:
            time.sleep(base_sleep * (2 ** i))
    return False, last_status, last_final, last_title


def find_latest_full_backup(data_dir: Path) -> Optional[Path]:
    backups = sorted(data_dir.glob("companies.validonly.bak.*.json"))
    if backups:
        return backups[-1]
    backups = sorted(data_dir.glob("companies.json.bak.*"))
    if backups:
        return backups[-1]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry pending 403/429 URLs and restore successful entries")
    parser.add_argument("--data", default="data/companies.json")
    parser.add_argument("--retry-queue", default="data/retry_queue.json")
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--base-sleep", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=20)
    args = parser.parse_args()

    data_path = Path(args.data)
    retry_path = Path(args.retry_queue)
    data_dir = data_path.parent

    if not data_path.exists():
        raise SystemExit(f"Missing data file: {data_path}")
    if not retry_path.exists():
        raise SystemExit(f"Missing retry queue: {retry_path}")

    checked_at = now_iso()

    companies: List[Dict[str, Any]] = json.loads(data_path.read_text(encoding="utf-8"))
    retry_items: List[Dict[str, Any]] = json.loads(retry_path.read_text(encoding="utf-8"))

    backup_source = find_latest_full_backup(data_dir)
    full_backup: List[Dict[str, Any]] = []
    if backup_source and backup_source.exists():
        full_backup = json.loads(backup_source.read_text(encoding="utf-8"))

    company_template: Dict[str, Dict[str, Any]] = {}
    tool_template: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for c in full_backup:
        cname = c.get("company", "")
        if not cname:
            continue
        company_template[cname] = c
        for t in c.get("tools", []):
            tname = t.get("name", "")
            if tname:
                tool_template[(cname, tname)] = t

    company_map = {c.get("company", ""): c for c in companies if c.get("company")}

    resolved: List[Dict[str, Any]] = []
    remaining: List[Dict[str, Any]] = []

    def run_item(item: Dict[str, Any]) -> Dict[str, Any]:
        cname = item.get("company", "")
        tname = item.get("tool", "")
        url = item.get("source_url", "")
        if not cname or not tname or not url:
            return {"item": item, "valid": False, "ok": False, "status": None, "final_url": "", "title": ""}

        ok, status, final_url, title = retry_check(
            url=url,
            attempts=args.attempts,
            timeout=args.timeout,
            base_sleep=args.base_sleep,
        )
        return {
            "item": item,
            "valid": True,
            "ok": ok,
            "status": status,
            "final_url": final_url,
            "title": title,
        }

    checked_results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_item, item) for item in retry_items]
        for fut in as_completed(futures):
            checked_results.append(fut.result())

    for out in checked_results:
        item = out["item"]
        cname = item.get("company", "")
        tname = item.get("tool", "")
        url = item.get("source_url", "")
        if not out["valid"]:
            item["checked_at"] = checked_at
            item["reason"] = "invalid_retry_item"
            remaining.append(item)
            continue

        ok = out["ok"]
        status = out["status"]
        final_url = out["final_url"]
        title = out["title"]

        if ok:
            if cname not in company_map:
                template = company_template.get(cname, {})
                company_map[cname] = {
                    "company": cname,
                    "domain": template.get("domain", ""),
                    "industry": template.get("industry", "Unknown"),
                    "tools": [],
                    "updated_at": datetime.now().date().isoformat(),
                }

            company = company_map[cname]
            tools = company.setdefault("tools", [])
            existing_idx = next((i for i, t in enumerate(tools) if t.get("name") == tname), None)
            base_tool = dict(tool_template.get((cname, tname), {"name": tname, "use_case": ""}))
            base_tool["source_url"] = final_url or url
            base_tool["final_url"] = final_url or url
            base_tool["http_status"] = status
            base_tool["checked_at"] = checked_at
            base_tool["evidence_title"] = title
            base_tool["verified"] = True
            if not base_tool.get("source_type"):
                base_tool["source_type"] = "vendor_case_study"

            if existing_idx is None:
                tools.append(base_tool)
            else:
                tools[existing_idx] = base_tool

            resolved.append(
                {
                    "company": cname,
                    "tool": tname,
                    "source_url": url,
                    "final_url": final_url or url,
                    "http_status": status,
                    "checked_at": checked_at,
                }
            )
        else:
            item["checked_at"] = checked_at
            item["http_status"] = status
            item["final_url"] = final_url
            item["reason"] = f"http_{status}" if status else "network_error"
            remaining.append(item)

    updated_companies = list(company_map.values())
    updated_companies.sort(key=lambda x: x.get("company", ""))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    before_backup = data_dir / f"companies.before_retry.{timestamp}.json"
    before_backup.write_text(data_path.read_text(encoding="utf-8"), encoding="utf-8")

    data_path.write_text(json.dumps(updated_companies, ensure_ascii=False, indent=2), encoding="utf-8")
    retry_path.write_text(json.dumps(remaining, ensure_ascii=False, indent=2), encoding="utf-8")

    resolved_path = data_dir / "retry_queue_resolved.json"
    report_path = data_dir / "retry_run_report.json"
    resolved_path.write_text(json.dumps(resolved, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "checked_at": checked_at,
        "attempts": args.attempts,
        "timeout": args.timeout,
        "workers": args.workers,
        "input_retry_items": len(retry_items),
        "resolved_items": len(resolved),
        "remaining_items": len(remaining),
        "before_backup": str(before_backup),
        "backup_source": str(backup_source) if backup_source else "",
        "resolved_path": str(resolved_path),
        "retry_queue_path": str(retry_path),
        "data_path": str(data_path),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
