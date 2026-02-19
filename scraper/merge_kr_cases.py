"""
Merge Korean collaboration tool cases into main companies database
"""

import json

# Load current companies
with open("data/companies.json", "r", encoding="utf-8") as f:
    current_companies = json.load(f)

# Load Korean cases
with open("data/kr_collab_cases.json", "r", encoding="utf-8") as f:
    kr_cases = json.load(f)

print(f"Current database: {len(current_companies)} companies")
print(f"Korean cases to add: {len(kr_cases)} cases")

# Create company map for efficient lookup
company_map = {c["company"].lower(): c for c in current_companies}

# Merge Korean cases
added_count = 0
updated_count = 0

for kr_case in kr_cases:
    company_name = kr_case["company"]
    company_name_lower = company_name.lower()

    # Build tool entry
    tool_entry = {
        "name": kr_case["tool"],
        "use_case": kr_case["use_case"],
        "source_url": kr_case["source_url"],
        "source_type": kr_case["source_type"],
        "verified": True
    }

    if company_name_lower in company_map:
        # Update existing company
        existing = company_map[company_name_lower]

        # Check if tool already exists
        existing_tools = [t["name"].upper() for t in existing["tools"]]
        if kr_case["tool"] not in existing_tools:
            existing["tools"].append(tool_entry)
            updated_count += 1
            print(f"  ✓ Updated {company_name} with {kr_case['tool']}")
    else:
        # Add new company
        new_company = {
            "company": company_name,
            "domain": kr_case.get("domain", ""),
            "industry": kr_case["industry"],
            "tools": [tool_entry],
            "updated_at": kr_case["updated_at"]
        }
        current_companies.append(new_company)
        added_count += 1
        print(f"  + Added {company_name} ({kr_case['tool']})")

# Save merged data
with open("data/companies.json", "w", encoding="utf-8") as f:
    json.dump(current_companies, f, ensure_ascii=False, indent=2)

print(f"\n📊 병합 완료:")
print(f"  - 새로 추가된 기업: {added_count}개")
print(f"  - 업데이트된 기업: {updated_count}개")
print(f"  - 총 기업 수: {len(current_companies)}개")
print(f"✅ 저장 완료 → data/companies.json")
