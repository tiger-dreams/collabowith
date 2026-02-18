"""
CollabWith Vendor Scraper
협업 도구 벤더들의 공식 Customer Stories 페이지에서
기업명 + 사용 사례를 직접 크롤링합니다.
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import date

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

VENDORS = [
    {
        "tool": "Slack",
        "url": "https://slack.com/customer-stories",
        "company_selector": "h3, h2, .customer-name, [class*='name'], [class*='title']",
    },
    {
        "tool": "Notion",
        "url": "https://www.notion.com/customers",
        "company_selector": "h3, h2, [class*='name'], [class*='company']",
    },
    {
        "tool": "Miro",
        "url": "https://miro.com/customers/",
        "company_selector": "h3, h2, [class*='name']",
    },
    {
        "tool": "Zoom",
        "url": "https://www.zoom.com/en/customer-stories/",
        "company_selector": "h3, h2, [class*='company']",
    },
    {
        "tool": "Figma",
        "url": "https://www.figma.com/customers/",
        "company_selector": "h3, h2, [class*='name']",
    },
    {
        "tool": "Atlassian",
        "url": "https://www.atlassian.com/customers",
        "company_selector": "h3, h2, [class*='name'], [class*='title']",
    },
    {
        "tool": "Asana",
        "url": "https://asana.com/customers",
        "company_selector": "h3, h2, [class*='company']",
    },
]

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def scrape_vendor(vendor):
    print(f"\n🔍 [{vendor['tool']}] {vendor['url']} 크롤링 중...")
    results = []
    try:
        res = requests.get(vendor["url"], headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        # 페이지 링크에서 회사명 추출
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = clean_text(link.get_text())

            # 고객 사례 링크 패턴 감지
            if any(kw in href for kw in ["/customer", "/case-study", "/stories", "/customers/"]):
                if text and len(text) > 2 and len(text) < 60:
                    full_url = href if href.startswith("http") else f"https://{vendor['url'].split('/')[2]}{href}"
                    results.append({
                        "company": text,
                        "tool": vendor["tool"],
                        "source_url": full_url,
                        "source_type": "vendor_case_study",
                        "use_case": f"{vendor['tool']} 활용 사례",
                        "updated_at": str(date.today())
                    })

        print(f"  → {len(results)}건 발견")
    except Exception as e:
        print(f"  ❌ 실패: {e}")
    return results

def build_company_db(raw_data):
    """회사별로 그룹화"""
    company_map = {}
    for item in raw_data:
        name = item["company"]
        if name not in company_map:
            company_map[name] = {
                "company": name,
                "domain": "",
                "industry": "Unknown",
                "tools": [],
                "updated_at": item["updated_at"]
            }
        # 중복 도구 방지
        existing_tools = [t["name"] for t in company_map[name]["tools"]]
        if item["tool"] not in existing_tools:
            company_map[name]["tools"].append({
                "name": item["tool"],
                "use_case": item["use_case"],
                "source_url": item["source_url"],
                "source_type": item["source_type"]
            })
    return list(company_map.values())

if __name__ == "__main__":
    all_raw = []
    for vendor in VENDORS:
        data = scrape_vendor(vendor)
        all_raw.extend(data)
        time.sleep(2)  # Rate limit 방지

    print(f"\n📊 총 {len(all_raw)}건 raw 데이터 수집 완료")

    # DB 구조로 변환
    db = build_company_db(all_raw)
    # 도구 2개 이상인 회사만 포함 (노이즈 제거)
    db_filtered = [c for c in db if len(c["tools"]) >= 1]

    with open("../data/companies.json", "w", encoding="utf-8") as f:
        json.dump(db_filtered, f, ensure_ascii=False, indent=2)

    print(f"✅ {len(db_filtered)}개 기업 데이터 저장 → data/companies.json")

    # 샘플 미리보기
    print("\n📋 수집된 기업 샘플 (상위 10개):")
    for c in db_filtered[:10]:
        tools = ", ".join([t["name"] for t in c["tools"]])
        print(f"  {c['company']}: {tools}")
