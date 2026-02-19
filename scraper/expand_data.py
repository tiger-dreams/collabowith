"""
CollabWith Data Expansion Script
Search for vendor case studies for Fortune 500 companies
"""

import json
import time
from datetime import date

# Load current companies
with open("../data/companies.json", "r", encoding="utf-8") as f:
    current_companies = json.load(f)

current_company_names = {c["company"].lower() for c in current_companies}
print(f"Current database: {len(current_companies)} companies")

# Fortune 500 companies (top 100 from the fetched list)
fortune_500 = [
    "Walmart", "Amazon", "UnitedHealth Group", "Apple", "CVS Health",
    "Berkshire Hathaway", "Alphabet", "Exxon Mobil", "McKesson", "Cencora",
    "JPMorgan Chase", "Costco Wholesale", "Cigna", "Microsoft", "Cardinal Health",
    "Chevron", "Bank of America", "General Motors", "Ford Motor", "Elevance Health",
    "Citigroup", "Meta Platforms", "Centene", "Home Depot", "Fannie Mae",
    "Walgreens Boots Alliance", "Kroger", "Phillips 66", "Marathon Petroleum", "Verizon Communications",
    "Nvidia", "Goldman Sachs Group", "Wells Fargo", "Valero Energy", "Comcast",
    "State Farm Insurance", "AT&T", "Freddie Mac", "Humana", "Morgan Stanley",
    "Target", "StoneX Group", "Tesla", "Dell Technologies", "PepsiCo",
    "Walt Disney", "United Parcel Service", "Johnson & Johnson", "FedEx", "Archer Daniels Midland",
    "Procter & Gamble", "Lowe's", "Energy Transfer", "RTX", "Albertsons",
    "Sysco", "Progressive", "American Express", "Lockheed Martin", "MetLife",
    "HCA Healthcare", "Prudential Financial", "Boeing", "Caterpillar", "Merck",
    "Allstate", "Pfizer", "IBM", "New York Life Insurance", "Delta Air Lines",
    "Publix Super Markets", "Nationwide", "TD Synnex", "United Airlines Holdings", "ConocoPhillips",
    "TJX", "AbbVie", "Enterprise Products Partners", "Charter Communications", "Performance Food Group",
    "American Airlines Group", "Capital One Financial", "Cisco Systems", "HP", "Tyson Foods",
    "Intel", "Oracle", "Broadcom", "Deere", "Nike",
    "Liberty Mutual Insurance Group", "Plains GP Holdings", "USAA", "Bristol-Myers Squibb", "Ingram Micro Holding",
    "General Dynamics", "Coca-Cola", "TIAA", "Travelers", "Eli Lilly"
]

# Find companies not in database
new_companies = [c for c in fortune_500 if c.lower() not in current_company_names]
print(f"\nCompanies to add: {len(new_companies)}")
for i, c in enumerate(new_companies[:20], 1):
    print(f"  {i}. {c}")

# Prepare search queries for each company
# We'll search for these companies with major SaaS vendors
vendors = ["Slack", "Notion", "Miro", "Zoom", "Figma", "Asana", "Monday", "Trello", "Jira", "Atlassian"]

print(f"\nTarget vendors: {len(vendors)}")
for v in vendors:
    print(f"  - {v}")

print(f"\nTotal potential searches: {len(new_companies)} × {len(vendors)} = {len(new_companies) * len(vendors)}")

# Save the list of companies to search
with open("companies_to_search.json", "w", encoding="utf-8") as f:
    json.dump({
        "companies": new_companies,
        "vendors": vendors,
        "total_combinations": len(new_companies) * len(vendors)
    }, f, ensure_ascii=False, indent=2)

print("\n✅ Saved search list to companies_to_search.json")
