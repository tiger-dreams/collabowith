"""
CollabWith LLM Extractor
회사명을 입력받아 웹에서 협업 도구 사용 기사를 검색하고,
Gemini LLM으로 구조화된 데이터를 추출합니다.
"""

import requests
import json
import re
import time
import sys
import os
from bs4 import BeautifulSoup
import google.generativeai as genai

# Gemini API 설정
GEMINI_API_KEY = "AIzaSyCTL0OobPOlkWvLOmqlXYtGbc5R4hZFfWA"
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120"
}

COLLAB_TOOLS = [
    "Slack", "Notion", "Zoom", "Miro", "Figma", "Jira", "Confluence",
    "Teams", "Microsoft Teams", "Google Meet", "Asana", "Monday.com",
    "Trello", "ClickUp", "Airtable", "Linear", "Loom", "Discord",
    "Webex", "Google Workspace", "GitHub", "GitLab", "Basecamp"
]


def search_articles(company: str) -> list[dict]:
    """Brave Search API로 협업 도구 관련 기사 검색"""
    query = f"{company} collaboration tools Slack Notion Zoom case study"
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": os.environ.get("BRAVE_API_KEY", "")
    }
    params = {"q": query, "count": 5, "search_lang": "en"}

    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()
        results = data.get("web", {}).get("results", [])
        return [{"url": r["url"], "title": r["title"], "snippet": r.get("description", "")}
                for r in results]
    except Exception as e:
        print(f"  검색 실패: {e}")
        return []


def fetch_article_text(url: str) -> str:
    """URL에서 본문 텍스트 추출"""
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        # 불필요한 태그 제거
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return text[:3000]  # 토큰 절약을 위해 3000자로 제한
    except Exception as e:
        return ""


def extract_with_llm(company: str, article_text: str, source_url: str) -> list[dict]:
    """Gemini로 협업 도구 정보 추출"""
    tools_list = ", ".join(COLLAB_TOOLS)
    prompt = f"""
다음 기사에서 '{company}' 기업이 사용하는 협업 도구 정보를 추출해줘.

협업 도구 목록 (이 중에서 찾아): {tools_list}

기사 내용:
{article_text}

결과를 아래 JSON 형식으로만 응답해줘 (다른 텍스트 없이):
[
  {{
    "tool": "도구명",
    "use_case": "어떤 용도로 사용하는지 한국어로 간결하게",
    "confidence": "high/medium/low"
  }}
]

협업 도구가 없으면 빈 배열 [] 반환.
"""
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        # JSON 파싱
        match = re.search(r'\[.*?\]', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        print(f"  LLM 추출 실패: {e}")
    return []


def process_company(company: str) -> dict:
    """회사 전체 파이프라인 실행"""
    print(f"\n🔍 [{company}] 처리 중...")
    articles = search_articles(company)
    print(f"  → 기사 {len(articles)}건 발견")

    all_tools = {}
    sources = []

    for art in articles[:3]:
        print(f"  📄 {art['title'][:60]}...")
        text = fetch_article_text(art["url"])
        if not text:
            continue
        tools = extract_with_llm(company, text, art["url"])
        for t in tools:
            if t.get("confidence") in ["high", "medium"] and t.get("tool"):
                key = t["tool"]
                if key not in all_tools:
                    all_tools[key] = {
                        "name": key,
                        "use_case": t["use_case"],
                        "source_url": art["url"],
                        "source_type": "media_article"
                    }
        sources.append(art["url"])
        time.sleep(1)

    result = {
        "company": company,
        "domain": f"{company.lower().replace(' ', '')}.com",
        "industry": "Unknown",
        "tools": list(all_tools.values()),
        "sources": sources,
        "updated_at": time.strftime("%Y-%m-%d")
    }
    print(f"  ✅ 협업 도구 {len(result['tools'])}개 추출 완료")
    return result


def save_to_db(new_entry: dict, db_path: str = "../data/sample.json"):
    """DB에 추가/업데이트"""
    try:
        with open(db_path, "r", encoding="utf-8") as f:
            db = json.load(f)
    except:
        db = []

    # 기존 항목 업데이트 또는 추가
    found = False
    for i, item in enumerate(db):
        if item["company"].lower() == new_entry["company"].lower():
            db[i] = new_entry
            found = True
            break
    if not found:
        db.append(new_entry)

    with open(db_path, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    print(f"  💾 DB 저장 완료 ({db_path})")


if __name__ == "__main__":
    companies = sys.argv[1:] if len(sys.argv) > 1 else ["Samsung", "LG", "Hyundai"]
    for company in companies:
        result = process_company(company)
        if result["tools"]:
            save_to_db(result)
        time.sleep(2)
    print("\n🎉 모든 처리 완료!")
