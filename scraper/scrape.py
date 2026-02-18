"""
Collabo Stack Scraper
협업 도구 벤더들의 고객 사례(Customer Stories) 페이지를 크롤링하여
기업별 협업 도구 사용 데이터를 수집합니다.
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

SOURCES = [
    {
        "tool": "Slack",
        "url": "https://slack.com/customer-stories",
        "selector": ".customer-story",
    },
    {
        "tool": "Notion",
        "url": "https://www.notion.so/customers",
        "selector": ".customer-card",
    },
    {
        "tool": "Miro",
        "url": "https://miro.com/customers/",
        "selector": ".case-study-card",
    },
    {
        "tool": "Zoom",
        "url": "https://www.zoom.com/en/customer-stories/",
        "selector": ".customer-card",
    },
    {
        "tool": "Figma",
        "url": "https://www.figma.com/customers/",
        "selector": ".customer-story",
    },
    {
        "tool": "Atlassian",
        "url": "https://www.atlassian.com/customers",
        "selector": ".customer-card",
    },
]


def scrape_page(source):
    """단일 소스 페이지 크롤링"""
    results = []
    try:
        res = requests.get(source["url"], headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        cards = soup.select(source["selector"])

        for card in cards[:20]:  # 최대 20개
            company = card.get_text(strip=True)[:100]
            results.append({
                "company": company,
                "tool": source["tool"],
                "source_url": source["url"],
                "scraped_at": datetime.now().isoformat(),
            })
    except Exception as e:
        print(f"[{source['tool']}] 스크래핑 실패: {e}")
    return results


def run():
    all_data = []
    for source in SOURCES:
        print(f"🔍 {source['tool']} 크롤링 중...")
        data = scrape_page(source)
        all_data.extend(data)
        print(f"  → {len(data)}건 수집")
        time.sleep(1.5)  # Rate limit 방지

    with open("../data/raw.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 총 {len(all_data)}건 저장 완료 → data/raw.json")


if __name__ == "__main__":
    run()
