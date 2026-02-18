import json, re, time, sys, os
import requests

# ── 설정 ──────────────────────────────────────────────
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
DB_PATH = "../data/companies.json"

# ❌ 제외할 도메인 (커뮤니티, 포럼, 뉴스 등)
BAD_DOMAINS = [
    "community.", "support.", "help.", "forum.", 
    "reddit.com", "news.ycombinator.com", "quora.com",
    "medium.com", "linkedin.com"
]

# ❌ 루트/목록 페이지 패턴
ROOT_PATTERNS = [
    r'/customers/?$', r'/customer-stories/?$', r'/customers/search',
    r'/en/customer-stories/?$', r'/intl/[^/]+/customer-stories/?$',
    r'/case-studies/?$'
]

# Brave API 키
def get_brave_key():
    try:
        cfg = json.load(open(os.path.expanduser("~/.openclaw/openclaw.json")))
        return cfg["tools"]["web"]["search"]["apiKey"]
    except:
        return os.environ.get("BRAVE_API_KEY", "")

BRAVE_KEY = get_brave_key()

# ── 유틸 ──────────────────────────────────────────────
def is_valid_url(url):
    """URL이 유효한 사례 링크인지 판별"""
    if not url: return False
    # Figma Community 제외
    if "figma.com/community" in url: return False
    
    if any(re.search(p, url) for p in ROOT_PATTERNS): return False
    if any(bd in url for bd in BAD_DOMAINS): return False
    return True

def check_http_status(url, timeout=6):
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code < 400: return True
    except: pass
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
        return r.status_code < 400
    except: return False

def brave_search(query, count=5):
    if not BRAVE_KEY: return []
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_KEY},
            params={"q": query, "count": count},
            timeout=10
        )
        return [{"url": i["url"], "title": i.get("title", "")} 
                for i in r.json().get("web", {}).get("results", [])]
    except: return []

# ── URL 탐색 로직 ──────────────────────────────────────
def find_url(company, tool):
    print(f"    🔍 검색 중: {company} + {tool}")
    
    # 전략 1: 공식 사례 (site: vendor)
    queries = [
        f'"{company}" {tool} case study customer story',
        f'"{company}" {tool} engineering blog tech stack',
        f'How {company} uses {tool}'
    ]
    
    for q in queries:
        results = brave_search(q, count=4)
        time.sleep(1.2)
        for r in results:
            url = r["url"]
            if is_valid_url(url):
                # 제목/URL에 회사명 포함 여부 확인 (느슨하게)
                c_slug = company.lower().replace(" ", "-")
                if (company.lower() in r["title"].lower()) or (c_slug in url.lower()):
                    if check_http_status(url):
                        return url, r["title"]
    return None, None

# ── 메인 실행 ──────────────────────────────────────────
def run():
    print(f"📂 DB 로드: {DB_PATH}")
    with open(DB_PATH, encoding="utf-8") as f:
        db = json.load(f)
    
    updates = 0
    for c in db:
        cname = c["company"]
        print(f"\n🏢 {cname}")
        for t in c["tools"]:
            tname = t["name"]
            url = t.get("source_url", "")
            
            # 1. 기존 URL 검증 (엄격하게)
            if is_valid_url(url):
                print(f"  ✅ {tname}: 유효함")
                t["verified"] = True
            else:
                # 2. 무효/미검증 → 재검색
                if url: print(f"  ❌ {tname}: URL 폐기 ({url}) → 재검색")
                else: print(f"  🔄 {tname}: URL 없음 → 검색")
                
                new_url, title = find_url(cname, tname)
                if new_url:
                    print(f"  🎉 발견: {new_url}")
                    t["source_url"] = new_url
                    t["source_title"] = title
                    t["verified"] = True
                    t["source_type"] = "media_article" if "blog" in new_url else "vendor_case_study"
                    updates += 1
                else:
                    print(f"  💨 실패: 적절한 URL 못 찾음")
                    t["verified"] = False
                    # 잘못된 URL은 아예 지워버림 (오염 방지)
                    if url and not is_valid_url(url):
                        t["source_url"] = ""

    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    print(f"\n💾 저장 완료 ({updates}건 업데이트)")

if __name__ == "__main__":
    run()
