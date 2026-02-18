"""
CollabWith URL Verifier & Enricher
- DB의 각 기업+도구 조합에 대해 Brave Search로 실제 사례 URL 탐색
- HTTP HEAD 요청으로 URL 존재 여부 검증
- 검증된 URL로 DB 업데이트
"""

import json, re, time, sys, os
import requests
from datetime import date

# ── 설정 ──────────────────────────────────────────────
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
DB_PATH = "../data/companies.json"
ROOT_PATTERNS = [
    r'/customers/?$', r'/customer-stories/?$', r'/customers/search',
    r'/en/customer-stories/?$', r'/intl/[^/]+/customer-stories/?$'
]

# Brave API 키 (OpenClaw 설정에서 읽기)
def get_brave_key():
    try:
        cfg = json.load(open(os.path.expanduser("~/.openclaw/openclaw.json")))
        return cfg["tools"]["web"]["search"]["apiKey"]
    except:
        return os.environ.get("BRAVE_API_KEY", "")

BRAVE_KEY = get_brave_key()

# ── 유틸 ──────────────────────────────────────────────
def is_root_url(url):
    return not url or any(re.search(p, url) for p in ROOT_PATTERNS)

def is_verified(url):
    return bool(url) and not is_root_url(url)

def check_url_exists(url, timeout=8):
    """HTTP HEAD로 URL 실재 여부 확인"""
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code < 400
    except:
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
            return r.status_code < 400
        except:
            return False

def brave_search(query, count=5):
    """Brave Search API 호출"""
    if not BRAVE_KEY:
        print("  ⚠️  Brave API 키 없음")
        return []
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_KEY},
            params={"q": query, "count": count},
            timeout=10
        )
        data = r.json()
        return [{"url": i["url"], "title": i.get("title", "")}
                for i in data.get("web", {}).get("results", [])]
    except Exception as e:
        print(f"  ⚠️  검색 실패: {e}")
        return []

# ── 핵심 로직 ──────────────────────────────────────────
VENDOR_DOMAINS = {
    "Slack":      "slack.com/customer-stories",
    "Notion":     "notion.com/customers",
    "Zoom":       "zoom.com",
    "Miro":       "miro.com/customers",
    "Figma":      "figma.com/customers",
    "Jira":       "atlassian.com/customers",
    "Confluence": "atlassian.com/customers",
    "Teams":      "microsoft.com/customers OR customers.microsoft.com",
    "Asana":      "asana.com/customers",
}

def find_verified_url(company, tool_name):
    """Brave Search로 특정 기업+도구 사례 URL 탐색 후 검증"""
    domain_hint = VENDOR_DOMAINS.get(tool_name, "")
    query = f'"{company}" {tool_name} case study customer story site:{domain_hint.split(" ")[0]}'
    print(f"    🔍 검색: {query[:80]}")
    results = brave_search(query, count=5)
    time.sleep(1.2)  # Rate limit

    for r in results:
        url = r["url"]
        if is_root_url(url):
            continue
        # 회사명이 URL에 포함되는지 확인 (간단 휴리스틱)
        company_slug = company.lower().replace(" ", "-").replace("/", "")
        if any(part in url.lower() for part in [company_slug, company.lower().split()[0]]):
            if check_url_exists(url):
                return url, r["title"]

    # 2차 시도: 더 넓은 검색
    query2 = f'{company} {tool_name} customer story'
    results2 = brave_search(query2, count=3)
    time.sleep(1.2)
    for r in results2:
        url = r["url"]
        if is_root_url(url) or is_verified(url) is False:
            continue
        vendor_domain = VENDOR_DOMAINS.get(tool_name, "").split(" ")[0]
        if vendor_domain and vendor_domain in url:
            if check_url_exists(url):
                return url, r["title"]

    return None, None

def enrich_db():
    """DB 전체를 순회하며 미검증 URL을 검증된 URL로 교체"""
    with open(DB_PATH, encoding="utf-8") as f:
        db = json.load(f)

    updated_count = 0
    for company in db:
        cname = company["company"]
        print(f"\n🏢 [{cname}]")
        for tool in company["tools"]:
            tname = tool["name"]
            current_url = tool.get("source_url", "")

            if is_verified(current_url):
                # 기존 URL이 검증된 경우 → HTTP 체크만
                exists = check_url_exists(current_url)
                if not exists:
                    print(f"  ❌ {tname}: 기존 URL 404 → 재검색 필요")
                    tool["verified"] = False
                else:
                    print(f"  ✅ {tname}: 기존 URL 유효 ({current_url[:60]})")
                    tool["verified"] = True
                time.sleep(0.3)
            else:
                # 루트 URL 또는 미검증 → 새 URL 탐색
                print(f"  🔄 {tname}: 미검증 → URL 탐색 중...")
                new_url, title = find_verified_url(cname, tname)
                if new_url:
                    print(f"  ✅ {tname}: 새 URL 발견 → {new_url[:70]}")
                    tool["source_url"] = new_url
                    tool["source_title"] = title
                    tool["verified"] = True
                    updated_count += 1
                else:
                    print(f"  ⚠️  {tname}: 검증된 URL 없음 (검증 중 유지)")
                    tool["verified"] = False

    # 저장
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 완료! {updated_count}개 URL 업데이트 → {DB_PATH}")
    return db

if __name__ == "__main__":
    print("=" * 60)
    print("CollabWith URL Verifier & Enricher")
    print("=" * 60)
    enrich_db()
