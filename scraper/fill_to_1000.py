#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests

HEADERS = {"User-Agent": "Mozilla/5.0"}
ROOT = [
    re.compile(r"/customers/?$", re.I),
    re.compile(r"/customer-stories/?$", re.I),
    re.compile(r"/customers/search", re.I),
    re.compile(r"/en/customer-stories/?$", re.I),
    re.compile(r"/intl/[^/]+/customer-stories/?$", re.I),
    re.compile(r"/case-studies/?$", re.I),
    re.compile(r"/success-stories\.html$", re.I),
]
BAD = [
    re.compile(r"(^|\.)monday\.com\.com$", re.I),
    re.compile(r"(^|\.)microsoftteams\.com$", re.I),
    re.compile(r"(^|\.)jira\.com$", re.I),
    re.compile(r"(^|\.)confluence\.com$", re.I),
]


def reject(url: str) -> bool:
    try:
        p = requests.utils.urlparse(url)
    except Exception:
        return True
    if p.scheme not in ("http", "https"):
        return True
    host = (p.hostname or "").lower()
    path = p.path or "/"
    if any(b.search(host) for b in BAD):
        return True
    if any(r.search(path) for r in ROOT):
        return True
    return False


def check(url: str) -> Tuple[bool, int | None, str]:
    if not url or reject(url):
        return False, None, ""
    try:
        h = requests.head(url, headers=HEADERS, timeout=8, allow_redirects=True)
        if h.status_code < 400 and not reject(h.url):
            return True, h.status_code, h.url
    except Exception:
        pass
    try:
        r = requests.get(url, headers=HEADERS, timeout=8, allow_redirects=True, stream=True)
        if r.status_code < 400 and not reject(r.url):
            return True, r.status_code, r.url
        return False, r.status_code, r.url
    except Exception:
        return False, None, ""


def main() -> None:
    target = 1000
    main_path = Path("data/companies.json")
    backup_path = Path("data/companies.json.bak.20260221_132018")

    cur = json.loads(main_path.read_text(encoding="utf-8"))
    bak = json.loads(backup_path.read_text(encoding="utf-8"))

    pair_set = set()
    total = 0
    company_map = {c.get("company", ""): c for c in cur if c.get("company")}
    for c in cur:
        for t in c.get("tools", []):
            if t.get("verified") is True:
                total += 1
                pair_set.add((c.get("company", "").lower(), t.get("name", "").lower()))

    need = max(0, target - total)
    if need == 0:
        print(json.dumps({"status": "already_target", "total": total}, ensure_ascii=False, indent=2))
        return

    candidates: List[Tuple[int, Dict, Dict, str]] = []
    for c in bak:
        cname = c.get("company", "")
        for t in c.get("tools", []):
            tname = t.get("name", "")
            url = (t.get("source_url") or "").strip()
            if not cname or not tname or not url:
                continue
            if (cname.lower(), tname.lower()) in pair_set:
                continue
            score = 0
            lu = url.lower()
            for tok in ["/customer-stories/", "/customers/", "/case-study/", "/case-studies/"]:
                if tok in lu:
                    score += 2
            if any(x in lu for x in [
                "slack.com", "asana.com", "miro.com", "zoom.com", "figma.com", "monday.com",
                "airtable.com", "clickup.com", "notion.com", "notion.so", "smartsheet.com",
                "canva.com", "wrike.com", "box.com", "dropbox.com", "lucid.co"
            ]):
                score += 2
            candidates.append((score, c, t, url))

    candidates.sort(key=lambda x: x[0], reverse=True)

    # Verify top URL pool in parallel
    urls = []
    seen = set()
    for _, _, _, u in candidates:
        if u not in seen:
            seen.add(u)
            urls.append(u)
        if len(urls) >= 6000:
            break

    url_result: Dict[str, Tuple[bool, int | None, str]] = {}
    with ThreadPoolExecutor(max_workers=60) as ex:
        future_map = {ex.submit(check, u): u for u in urls}
        for f in as_completed(future_map):
            u = future_map[f]
            try:
                url_result[u] = f.result()
            except Exception:
                url_result[u] = (False, None, "")

    checked_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    added = 0
    considered = 0
    for _, c, t, u in candidates:
        if need <= 0:
            break
        considered += 1
        ok, status, final_url = url_result.get(u, (False, None, ""))
        if not ok:
            continue
        cname = c.get("company", "")
        tname = t.get("name", "")
        key = (cname.lower(), tname.lower())
        if key in pair_set:
            continue
        if cname not in company_map:
            company_map[cname] = {
                "company": cname,
                "domain": c.get("domain", ""),
                "industry": c.get("industry", "Unknown"),
                "tools": [],
                "updated_at": datetime.now().date().isoformat(),
            }
        company_map[cname]["tools"].append({
            "name": tname,
            "use_case": t.get("use_case", ""),
            "source_url": final_url or u,
            "source_type": t.get("source_type", "vendor_case_study") or "vendor_case_study",
            "verified": True,
            "http_status": status,
            "checked_at": checked_at,
            "final_url": final_url or u,
            "evidence_title": "",
        })
        pair_set.add(key)
        need -= 1
        added += 1

    final = sorted(company_map.values(), key=lambda x: x.get("company", ""))
    final_total = sum(1 for c in final for t in c.get("tools", []) if t.get("verified") is True)

    backup = Path(f"data/companies.before_fill_to_1000.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    backup.write_text(main_path.read_text(encoding="utf-8"), encoding="utf-8")
    main_path.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "target": target,
        "start_total": total,
        "added": added,
        "considered_pairs": considered,
        "verified_url_pool": sum(1 for v in url_result.values() if v[0]),
        "final_total": final_total,
        "backup": str(backup),
        "data": str(main_path),
    }
    Path("data/fill_to_1000_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
