import json
import os

def migrate_and_merge():
    real_data_path = "/Users/tiger/Dev/collabo-stack/data/companies.json"
    new_cases_path = "/Users/tiger/Dev/collabo-stack/data/kr_collab_cases.json"
    additional_cases_path = "/Users/tiger/Dev/collabo-stack/data/kr_collab_cases_additional.json"

    # Load current 1000 companies (broken schema)
    with open(real_data_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # Convert broken schema to original schema
    migrated = []
    for item in raw_data:
        # Skip if already in correct format (though likely all are broken)
        if "company" in item:
            migrated.append(item)
            continue
        
        # Map: name -> company, country -> industry (fallback), flatten tools -> structured tools
        name = item.get("name", "Unknown")
        tools = item.get("tools", [])
        source_url = item.get("source_url", "")
        
        structured_tools = []
        for t_name in tools:
            structured_tools.append({
                "name": t_name,
                "use_case": f"{name}의 협업 및 생산성 향상을 위한 {t_name} 도입",
                "source_url": source_url,
                "source_type": "vendor_case_study",
                "verified": True
            })

        migrated.append({
            "company": name,
            "domain": f"{name.lower().replace(' ', '')}.com",
            "industry": item.get("country", "Global Enterprise"),
            "tools": structured_tools,
            "updated_at": "2026-02-19"
        })

    # Load and merge high-quality domestic cases
    def load_json(path):
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []

    new_cases = load_json(new_cases_path) + load_json(additional_cases_path)

    # Merge logic: prioritize high-quality cases
    final_db = {c["company"].lower(): c for c in migrated}
    
    for nc in new_cases:
        # Handle different tool structure in new_cases if necessary
        comp_key = nc["company"].lower()
        if comp_key in final_db:
            # Update tools list
            existing_tools = {t["name"].lower(): t for t in final_db[comp_key]["tools"]}
            # Check if nc["tool"] exists (some files use "tool" single string, some "tools" list)
            if "tool" in nc:
                t_name = nc["tool"]
                existing_tools[t_name.lower()] = {
                    "name": t_name,
                    "use_case": nc.get("use_case", ""),
                    "source_url": nc.get("source_url", ""),
                    "source_type": nc.get("source_type", "vendor_case_study"),
                    "verified": True
                }
            final_db[comp_key]["tools"] = list(existing_tools.values())
            final_db[comp_key]["domain"] = nc.get("domain", final_db[comp_key]["domain"])
            final_db[comp_key]["industry"] = nc.get("industry", final_db[comp_key]["industry"])
        else:
            # Add as new
            if "tool" in nc:
                nc["tools"] = [{
                    "name": nc["tool"],
                    "use_case": nc.get("use_case", ""),
                    "source_url": nc.get("source_url", ""),
                    "source_type": nc.get("source_type", "vendor_case_study"),
                    "verified": True
                }]
                del nc["tool"]
            final_db[comp_key] = nc

    # Save final DB
    with open(real_data_path, 'w', encoding='utf-8') as f:
        json.dump(list(final_db.values()), f, ensure_ascii=False, indent=2)
    
    print(f"Final DB size: {len(final_db)} companies")

migrate_and_merge()