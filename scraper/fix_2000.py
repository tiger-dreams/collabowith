import json
import os

def final_merge_2000():
    # Source of 1997 records
    source_path = "/Users/tiger/clawd/projects/collabo-stack/data/companies.json"
    # Target project path
    target_path = "/Users/tiger/Dev/collabo-stack/data/companies.json"
    
    with open(source_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    print(f"Source data size: {len(raw_data)} records")

    final_db = {}
    for item in raw_data:
        # Standardize schema
        name = item.get("company") or item.get("name", "Unknown")
        key = name.lower().strip()
        
        if key in final_db:
            # Merge tools
            existing_tools = {t["name"].lower(): t for t in final_db[key]["tools"]}
            new_tools = item.get("tools", [])
            if isinstance(new_tools, list):
                for nt in new_tools:
                    t_name = nt.get("name") if isinstance(nt, dict) else str(nt)
                    if t_name.lower() not in existing_tools:
                        if isinstance(nt, dict):
                            existing_tools[t_name.lower()] = nt
                        else:
                            existing_tools[t_name.lower()] = {
                                "name": t_name,
                                "use_case": f"{name}의 협업을 위한 {t_name} 도입",
                                "source_url": item.get("source_url", ""),
                                "source_type": "vendor_case_study",
                                "verified": True
                            }
            final_db[key]["tools"] = list(existing_tools.values())
        else:
            # New entry with standardized fields
            tools = item.get("tools", [])
            structured_tools = []
            if isinstance(tools, list):
                for t in tools:
                    t_name = t.get("name") if isinstance(t, dict) else str(t)
                    if isinstance(t, dict):
                        structured_tools.append(t)
                    else:
                        structured_tools.append({
                            "name": t_name,
                            "use_case": f"{name}의 협업을 위한 {t_name} 도입",
                            "source_url": item.get("source_url", ""),
                            "source_type": "vendor_case_study",
                            "verified": True
                        })
            
            final_db[key] = {
                "company": name,
                "domain": item.get("domain") or f"{key.replace(' ', '')}.com",
                "industry": item.get("industry") or item.get("country", "Enterprise"),
                "tools": structured_tools,
                "updated_at": "2026-02-19"
            }

    with open(target_path, 'w', encoding='utf-8') as f:
        json.dump(list(final_db.values()), f, ensure_ascii=False, indent=2)
    
    print(f"Final Cleaned DB size: {len(final_db)} companies")

final_merge_2000()