"""
Korean Collaboration Tools Scraper
한국 협업 툴들의 고객 사례를 수집합니다.
"""

import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import date

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

KR_VENDORS = [
    {
        "tool": "JANDI",
        "url": "https://blog.jandi.com/ko/category/user_case/",
        "base_url": "https://blog.jandi.com",
        "company_selector": "a",
        "link_pattern": "/ko/"
    },
    {
        "tool": "Dooray",
        "url": "https://helpdesk.dooray.com/share/pages/9wWo-xwiR66BO5LGshgVTg/2962315498932384699",
        "base_url": "https://dooray.com",
    },
    {
        "tool": "NAVER WORKS",
        "url": "https://naver.worksmobile.com/cases/",
        "base_url": "https://naver.worksmobile.com",
    },
    {
        "tool": "MailPlug",
        "url": "https://groupware.mailplug.com/",
        "base_url": "https://mailplug.com",
    },
]

def clean_text(text):
    return text.strip()

def scrape_jandi():
    """JANDI 블로그에서 고객 사례 수집"""
    print(f"\n🔍 [JANDI] 고객 사례 수집 중...")
    results = []
    try:
        res = requests.get("https://blog.jandi.com/ko/category/user_case/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        # 링크에서 고객 사례 추출
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = clean_text(link.get_text())

            # 고객 사례 링크 패턴
            if "/ko/" in href and ("customercase" in href or "user_case" in href):
                if text and len(text) > 5 and len(text) < 100:
                    # 회사명 추출 시도 (대괄호로 감싸진 경우)
                    if "[" in text and "]" in text:
                        industry = text.split("[")[1].split("]")[0]
                    else:
                        industry = "Unknown"

                    full_url = href if href.startswith("http") else f"https://blog.jandi.com{href}"

                    # 회사명 추출 (제목에서)
                    company_name = extract_company_from_title(text)

                    if company_name:
                        results.append({
                            "company": company_name,
                            "tool": "JANDI",
                            "source_url": full_url,
                            "source_type": "vendor_case_study",
                            "use_case": f"{industry} 업무 협업 및 커뮤니케이션",
                            "industry": industry,
                            "updated_at": str(date.today())
                        })

        print(f"  → {len(results)}건 발견")
    except Exception as e:
        print(f"  ❌ 실패: {e}")
    return results

def extract_company_from_title(title):
    """제목에서 회사명 추출 시도"""
    # JANDI 고객 사례 패턴: [[산업] 회사명 ~]
    if "[" in title and "]" in title:
        after_bracket = title.split("]", 1)[1] if "]" in title else title
        # 첫 번째 공백까지를 회사명으로 가정
        parts = after_bracket.strip().split()
        if parts:
            return parts[0]
    return None

def scrape_naver_works():
    """NAVER WORKS 도입사례 수집"""
    print(f"\n🔍 [NAVER WORKS] 도입사례 수집 중...")
    results = []
    try:
        res = requests.get("https://naver.worksmobile.com/cases/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        # 제목에서 회사명 추출
        for heading in soup.find_all(["h2", "h3", "h4"]):
            text = clean_text(heading.get_text())

            # 대문자로 시작하는 회사명 패턴
            if text and len(text) > 3 and len(text) < 60:
                if any(c.isupper() for c in text) or "도입" in text or "사례" in text:
                    results.append({
                        "company": text.split()[0] if text.split() else text,
                        "tool": "NAVER WORKS",
                        "source_url": "https://naver.worksmobile.com/cases/",
                        "source_type": "vendor_case_study",
                        "use_case": "업무용 메신저 및 협업 플랫폼",
                        "industry": "Various",
                        "updated_at": str(date.today())
                    })

        print(f"  → {len(results)}건 발견")
    except Exception as e:
        print(f"  ❌ 실패: {e}")
    return results

def main():
    print("🇰🇷 한국 협업 툴 고객 사례 수집 시작\n")

    all_data = []

    # JANDI 수집
    jandi_data = scrape_jandi()
    all_data.extend(jandi_data)
    time.sleep(2)

    # NAVER WORKS 수집
    naver_works_data = scrape_naver_works()
    all_data.extend(naver_works_data)
    time.sleep(2)

    # 결과 저장
    output_file = "../data/kr_collab_cases.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n📊 총 {len(all_data)}건 수집 완료")
    print(f"✅ 저장 → {output_file}")

    # 샘플 출력
    print("\n📋 수집된 기업 샘플:")
    for item in all_data[:10]:
        print(f"  • {item['company']} - {item['tool']} ({item['industry']})")

if __name__ == "__main__":
    main()
