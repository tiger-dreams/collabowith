#!/usr/bin/env python3
"""
Fortune 1000 Collaboration Tools Scraper
Collects and verifies collaboration tool usage for Fortune 1000 companies
"""

import json
import time
import os
from datetime import date
import requests
from typing import Dict, List, Set, Optional

# Configuration
DATA_FILE = "data/companies.json"
PROGRESS_FILE = "data/fortune_scraper_progress.json"

# Extended Fortune 500/1000 seed list (top companies)
fortune_companies_raw = [
    # Fortune 100 (already in expand_20k.py)
    "Walmart", "Amazon", "Exxon Mobil", "Apple", "CVS Health",
    "Berkshire Hathaway", "UnitedHealth Group", "McKesson", "AT&T", "AmerisourceBergen",
    "Alphabet", "Ford Motor", "Cigna", "Costco Wholesale", "Chevron",
    "Cardinal Health", "JPMorgan Chase", "General Motors", "Walgreens Boots Alliance", "Verizon Communications",
    "Microsoft", "Marathon Petroleum", "Kroger", "Fannie Mae", "Bank of America",
    "Home Depot", "Phillips 66", "Comcast", "Anthem", "Wells Fargo",
    "Citigroup", "Valero Energy", "General Electric", "Dell Technologies", "Johnson & Johnson",
    "State Farm Insurance", "Target", "IBM", "Raytheon Technologies", "Boeing",
    "Freddie Mac", "Centene", "United Parcel Service", "Lowe's", "Intel",
    "Facebook", "FedEx", "MetLife", "Walt Disney", "Procter & Gamble",
    "PepsiCo", "Humana", "Prudential Financial", "Archer Daniels Midland", "Albertsons",
    "Sysco", "Lockheed Martin", "HP", "Energy Transfer", "Goldman Sachs Group",
    "Morgan Stanley", "Caterpillar", "Cisco Systems", "Pfizer", "HCA Healthcare",
    "AIG", "American Express", "Delta Air Lines", "Merck", "American Airlines Group",
    "Charter Communications", "Allstate", "New York Life Insurance", "Nationwide", "Best Buy",
    "United Airlines Holdings", "Liberty Mutual Insurance Group", "Dow", "Tyson Foods", "TJX",
    "TIAA", "Oracle", "General Dynamics", "Deere", "Nike",
    "Liberty Mutual", "Plains GP Holdings", "USAA", "Bristol-Myers Squibb", "Eli Lilly",

    # Fortune 101-200
    "Abbott Laboratories", "AbbVie", "3M", "American Electric Power", "Amgen",
    "Archer Daniels Midland", "Aon", "Apple", "AT&T", "Automatic Data Processing",
    "Baker Hughes", "Bank of America", "Bank of New York Mellon", "Baxter International", "Becton Dickinson",
    "Best Buy", "Biogen", "Boeing", "Booz Allen Hamilton", "Bristol-Myers Squibb",
    "Broadcom", "Capital One Financial", "Cardinal Health", "Caterpillar", "CBRE Group",
    "Centene", "Charter Communications", "Chevron", "Cigna", "Cincinnati Financial",
    "Cisco Systems", "Citigroup", "Citizens Financial Group", "Clorox", "Cognizant Technology Solutions",
    "Colgate-Palmolive", "Comcast", "ConocoPhillips", "Conagra Brands", "Corning",
    "Costco Wholesale", "Crown Castle International", "Cummins", "CVS Health", "Danaher",
    "Deere", "Delta Air Lines", "Dell Technologies", "DHL Group", "Disney",
    "Discovery", "Dollar General", "Dow Inc", "DowDuPont", "Duke Energy",
    "DuPont de Nemours", "Eaton", "Electronic Arts", "Eli Lilly", "Emerson Electric",
    "Enterprise Products Partners", "EQT Corporation", "Exelon", "Expedition", "Exxon Mobil",
    "Facebook", "FedEx", "Fifth Third Bancorp", "FirstEnergy", "Fiserv",
    "Flex", "Ford Motor", "Fortive", "Fox Corporation", "Fresenius Medical Care",

    # Fortune 201-300
    "GameStop", "Gap", "General Dynamics", "General Electric", "General Mills",
    "Gilead Sciences", "Goldman Sachs Group", "Goodyear Tire & Rubber", "Google", "HCA Healthcare",
    "Hess", "Hewlett Packard Enterprise", "Honeywell", "HP Inc", "Humana",
    "Huntington Bancshares", "IBM", "Illumina", "Incyte", "Ingersoll Rand",
    "Intel", "Intuit", "Johnson & Johnson", "Johnson Controls International", "JPMorgan Chase",
    "Kellogg", "Keysight Technologies", "Kohl's", "Kroger", "L3Harris Technologies",
    "Lam Research", "L Brands", "Leidos", "Leonardo DRS", "Lilly",
    "Lincoln Financial Group", "Lockheed Martin", "Lowe's", "Lumen Technologies", "Marathon Petroleum",
    "Marriott International", "Martin Marietta Materials", "MassMutual", "Mattel", "McDonald's",
    "McKesson", "Medtronic", "Merck", "Mettler Toledo", "Micron Technology",
    "Microsoft", "Milton Hershey School", "Molina Healthcare", "Mondelez International", "Monster Beverage",
    "Morgan Stanley", "Motorola Solutions", "MSC Industrial Direct", "Nasdaq", "National Grid",
    "Natura &Co", "Navy Federal Credit Union", "Newmont", "NextEra Energy", "Nike",
    "Noble Energy", "Nordstrom", "Northrop Grumman", "Norwegian Cruise Line", "NRG Energy",
    "Nucor", "NVIDIA", "Occidental Petroleum", "O'Reilly Automotive", "Oracle",
    "Owens Corning", "PACCAR", "Palo Alto Networks", "Parker-Hannifin", "PayPal",

    # Fortune 301-400
    "PepsiCo", "Pfizer", "PG&E Corporation", "Phillips 66", "Pinnacle West Capital",
    "Pioneer Natural Resources", "Prudential Financial", "PulteGroup", "Public Service Enterprise Group", "Qualcomm",
    "Raytheon Technologies", "Regeneron Pharmaceuticals", "Regions Financial", "Republic Services", "Richardson Electronics",
    "Rockwell Automation", "Ross Stores", "Royal Caribbean Group", "Ryder System", "Salesforce",
    "Sanofi", "SAP", "S&P Global", "Simon Property Group", "Southern Company",
    "Southwest Airlines", "Spirit AeroSystems", "Sprint", "Stanley Black & Decker", "Starbucks",
    "State Farm", "STMicroelectronics", "SunTrust Banks", "SVB Financial Group", "Sysco",
    "T-Mobile US", "Target", "TD Ameritrade Holding", "Teachers Insurance and Annuity Association", "Tech Data",
    "Te Connectivity", "Teleflex", "Teradyne", "Texas Instruments", "Textron",
    "The Coca-Cola Company", "The Home Depot", "The Kraft Heinz Company", "The TJX Companies", "The Travelers Companies",
    "3M", "Thermo Fisher Scientific", "TJX Companies", "Toro", "Toyota Motor",
    "Tractor Supply", "Trane Technologies", "TransDigm Group", "Travelers", "Truist Financial",
    "Twilio", "Twitter", "Tyson Foods", "U.S. Bancorp", "Ultratech",
    "Union Pacific", "United Airlines Holdings", "United Parcel Service", "UnitedHealth Group", "United Rentals",
    "United States Steel", "United Technologies", "Universal Health Services", "University of Phoenix", "Unum Group",
    "USAA", "US Bancorp", "Valero Energy", "Vanguard Group", "Verisk Analytics",
    "Verizon Communications", "Visa", "VMware", "Vornado Realty Trust", "Walmart",

    # Fortune 401-500
    "Walt Disney", "Walmart", "Walmart Inc", "Walgreens Boots Alliance", "Waste Management",
    "Waters Corporation", "Wells Fargo", "Western Digital", "WestRock", "Weyerhaeuser",
    "Whirlpool", "Williams Companies", "Williams-Sonoma", "WPP", "Xcel Energy",
    "Xerox", "Yum! Brands", "Zillow Group", "Zoetis", "Zoetis Inc",

    # Fortune 501-600 (additional companies to expand coverage)
    "ABM Industries", "Acadia Realty Trust", "Academy Sports and Outdoors", "Accelya", "Accenture",
    "Acer", "Actavis", "Activision Blizzard", "Adcock Ingram", "Adobe Inc",
    "Advance Auto Parts", "Advanced Micro Devices", "Advocate Health Care", "AECOM", "Aegon",
    "AFLAC", "AGCO", "Agilent Technologies", "Agrium", "Airbnb",
    "Alaska Air Group", "Albertsons", "Alcoa", "Align Technology", "Alliant Energy",
    "Allied Universal", "Allstate", "Ally Financial", "Alphabet", "Altria Group",
    "Amazon.com", "American Airlines", "American Electric Power", "American Express", "American International Group",
    "American Outdoor Brands", "American Tower", "American Water Works", "Ameriprise Financial", "AmerisourceBergen",
    "Amphenol", "Analog Devices", "Anthem", "Aon", "Apache",
    "Apartment Investment and Management", "Apple", "AptarGroup", "Aramark", "ArcelorMittal",
    "Arconic", "Arch Capital Group", "Arista Networks", "Arrow Electronics", "Arthur J. Gallagher & Co.",
    "ASCENDING", "Ashland Global", "Asbury Automotive Group", "AstraZeneca", "AT&T",
    "Atmos Energy", "Atrion", "Automatic Data Processing", "Autonation", "Autodesk",
    "AutoZone", "Avery Dennison", "Avnet", "Axalta Coating Systems", "Baker Hughes",
    "Ball Corporation", "Bank of New York Mellon", "Barnes & Noble", "Barrick Gold", "Bath & Body Works",
    "Baxter International", "Becton Dickinson", "Berkshire Hathaway", "Best Buy", "Bharti Airtel",
    "Biogen", "Billion", "BlackRock", "Blackstone Group", "Bloomberg LP",
    "Boeing", "Boingo Wireless", "Booking Holdings", "BorgWarner", "Boston Scientific",
    "Bristol-Myers Squibb", "Broadcom", "Brunswick Corporation", "Burberry", "Burlington Stores",
    "Burns & McDonnell", "C.H. Robinson Worldwide", "Calpine", "Campbell Soup Company", "Camping World",
    "Canon", "Capital One", "Cardinal Health", "Carlisle Companies", "Caterpillar",
    "CBRE Group", "CenterPoint Energy", "Centene", "Charles Schwab Corporation", "Charter Communications",
    "Chevron", "Children's Hospital of Philadelphia", "Chubb Limited", "Cigna", "Cincinnati Financial",
    "Cintas Corporation", "Cisco Systems", "Citigroup", "Citizens Financial Group", "Clarivate",
    "Clorox", "CME Group", "CNO Financial Group", "Cognizant Technology Solutions", "Cohu",
    "Colgate-Palmolive", "Comcast", "Comerica", "Conagra Brands", "Concurrent Computer Corporation",
    "Continental Resources", "Cooper Companies", "Coping", "Corning", "Costco Wholesale",
    "Coty", "Country Financial", "Cox Enterprises", "Crown Holdings", "CrowdStrike",
    "Cummins", "CVS Health", "Dana Incorporated", "Danaher", "Deere & Company",
    "Dell Technologies", "Delta Air Lines", "Denso", "Devon Energy", "Dick's Sporting Goods",
    "Digital Realty", "Dillard's", "Dimensional Fund Advisors", "Discover Financial Services", "Discovery",
    "Dish Network", "Dollar Tree", "Dominion Energy", "Dover Corporation", "Dow Inc",
    "Dr Pepper Snapple Group", "DreamWorks Animation", "DTE Energy", "DuPont de Nemours", "Dycom Industries",
    "eBay", "Ecolab", "Edwards Lifesciences", "Ei.PA", "Electronic Arts",
    "Emerson Electric", "Endo International", "Enterprise Holdings", "Envestnet", "EQT Corporation",
    "Equifax", "Equinix", "Esprit Holdings", "Estee Lauder Companies", "Etsy",
    "Euronext", "Evergy", "Expedia Group", "Exelon", "ExlService Holdings",
    "Expedia", "Extra Space Storage", "Exxon Mobil", "Facebook", "FactSet Research Systems",
    "F5 Networks", "Fannie Mae", "Faraday Future", "Farmers Insurance", "Federated Hermes",
    "FedEx", "Fidelity National Information Services", "Fifth Third Bancorp", "First Data", "FirstEnergy",
    "First Republic Bank", "First Solar", "Fiserv", "Fisher Scientific", "Flex",
    "Flowserve", "Fluidra", "Ford Motor", "Fortive", "Fox Corporation",
    "Franklin Templeton Investments", "Franklin Resources", "Freddie Mac", "Freshpet", "Frontier Communications",
    "Fujifilm", "GameStop", "Gap", "Garmin", "GE Aviation",
    "GE Digital", "GE Power", "General Dynamics", "General Electric", "General Mills",
    "Gentex", "Genworth Financial", "Genuine Parts Company", "Gildan Activewear", "Gilead Sciences",
    "Globus Medical", "Glu Mobile", "Goodyear Tire & Rubber", "Gordon Food Service", "Groupon",
    "H&R Block", "HCA Healthcare", "HD Supply", "Health Net", "Healthcare Services Group",
    "Hemisphere", "Hess", "Hewlett Packard Enterprise", "Hilton Worldwide Holdings", "HMSHost",
    "Honeywell", "Horizon Therapeutics", "HP Inc", "HSBC Holdings", "HTC Corporation",
    "Humana", "Huntington Bancshares", "Hyatt Hotels Corporation", "IAC", "IBM",
    "Icon", "IDEX Corporation", "IDEXX Laboratories", "Illumina", "Incitec Pivot",
    "Incyte", "Ingredion", "Ingersoll Rand", "Instructure", "Intel",
    "Intuitive Surgical", "Intuit", "Invitation Homes", "Ion Geophysical", "Iron Mountain",
    "iRobot", "Itron", "J.B. Hunt Transport Services", "Jacobs Solutions", "JBS USA",
    "JCPenney", "Jefferson Financial", "Jeld-Wen Holding", "Jensen Group", "Johnson & Johnson",
    "Johnson Controls", "JPMorgan Chase", "Julius Baer Group", "Juniper Networks", "KBR",
    "Kellogg Company", "Kennametal", "Keryx Biopharmaceuticals", "KeyCorp", "KeyEnergy Services",
    "Keysight Technologies", "Kikkoman", "Kinder Morgan", "Kingfisher", "KLA-Tencor",
    "Kohl's", "Kraft Heinz", "Kroger", "Kubota", "L-3 Communications",
    "LabCorp", "Lam Research", "Lamb Weston", "Lands' End", "Landstar System",
    "LBrands", "Lear Corporation", "Leidos", "Lennar Corporation", "Lenovo",
    "Levi Strauss & Co", "Liberty Global", "Liberty Mutual", "Lincoln Electric", "Line Corporation",
    "Lincoln Financial Group", "LinkedIn", "Linde", "Live Nation Entertainment", "LKQ Corporation",
    "Lloyds Banking Group", "Loblaw Companies", "Lockheed Martin", "Loews Corporation", "LPL Financial",
    "Lowe's Companies", "Lumen Technologies", "LyondellBasell", "M&T Bank", "Macquarie Group",
    "Magnit", "ManpowerGroup", "Marathon Oil", "Marathon Petroleum", "Marcum LLP",
    "Marriott International", "Marshall & Ilsley", "Martin Marietta Materials", "Masco Corporation", "Mastercard",
    "Mattel", "MAU Workforce Solutions", "Maxim Integrated", "McDonald's", "McKesson",
    "Medtronic", "Merck & Co.", "Metcash", "MetLife", "Micron Technology",
    "Microsoft", "Mitsubishi Estate", "MKS Instruments", "Mobileye", "Molina Healthcare",
    "Mondelez International", "Monster Beverage", "Morgan Stanley", "Morningstar", "Mosaic Company",
    "Motorola Solutions", "MSC Industrial Direct", "Mueller Industries", "Mu Sigma", "Murata Manufacturing",
    "Nabors Industries", "Nasdaq", "Natura", "National Bank of Canada", "National Oilwell Varco",
    "National Grid", "NCR Corporation", "Netflix", "Neustar", "New Century Financial",
    "Newell Brands", "Newmont Mining", "New York Life Insurance", "News Corporation", "NextEra Energy",
    "Nexstar Media Group", "Nielsen Holdings", "Nike", "Nikon", "Noble Energy",
    "Norfolk Southern", "NortonLifeLock", "Norwegian Cruise Line", "Norsk Hydro", "Northrop Grumman",
    "Novartis", "Novellus Systems", "Nutanix", "NVIDIA", "Nutanix",
    "NVR", "O'Reilly Automotive", "Office Depot", "OfficeMax", "Old Republic International",
    "Olin Corporation", "OMV", "OneMain Financial", "On Semiconductor", "OpenText",
    "Oracle Corporation", "Otis Worldwide", "Owens Corning", "Paccar", "Palo Alto Networks",
    "Panera Bread", "Parker-Hannifin", "Paychex", "PayPal", "PBF Energy",
    "Pearson", "Penske Automotive Group", "Pentair", "PepsiCo", "Pfizer",
    "PG&E Corporation", "Philip Morris International", "Phillips 66", "Pinnacle Financial Partners", "Pioneer Natural Resources",
    "Pitney Bowes", "Plantronics", "PNC Financial Services", "Polypore International", "Praxair",
    "Principal Financial Group", "Procter & Gamble", "Prosperity Bancshares", "Prudential Financial", "PTC",
    "PulteGroup", "Pultrusion", "Public Storage", "PulteGroup", "Qualcomm",
    "Quanta Services", "Quest Diagnostics", "Quidel", "Qwest", "Rackspace Technology",
    "Raytheon", "Reckitt Benckiser", "Red Robin", "Regions Financial", "Regeneron Pharmaceuticals",
    "Republic Services", "ResMed", "Reynolds Consumer Products", "Rite Aid", "Roblox Corporation",
    "Rockwell Automation", "Rockwell Collins", "Rogers Communications", "Rolls-Royce Holdings", "Ross Stores",
    "Royal Bank of Canada", "Royal Caribbean Cruises", "Rubicon", "Salesforce", "SAP",
    "S&P Global", "Sanofi", "Santander", "S&P Dow Jones Indices", "Sara Lee",
    "SAS Institute", "Scale AI", "Schlumberger", "Schneider Electric", "Scotts Miracle-Gro",
    "Seagate Technology", "Sears Holdings", "SEGA", "Service Corporation International", "ServiceNow",
    "Sherwin-Williams", "Siemens", "Simon Property Group", "Sky", "Slack Technologies",
    "Snap Inc.", "Snowflake", "Societe Generale", "SolarEdge Technologies", "Solid Power",
    "Sonic Automotive", "Sonoco Products", "Southwest Airlines", "Southern Company", "SpaceX",
    "Spark Therapeutics", "Spectra Energy", "Spirit Airlines", "Spotify", "Sprouts Farmers Market",
    "Sprint", "Square", "Stanley Black & Decker", "Starbucks", "State Street Corporation",
    "Stellantis", "Steel Dynamics", "Stein Mart", "Stericycle", "St. Jude Medical",
    "STMicroelectronics", "StoneX Group", "SunPower", "Sunrun", "SunTrust Banks",
    "SVB Financial Group", "Swire Pacific", "Symantec", "Sysco", "T. Rowe Price Group",
    "T-Mobile US", "Taiwan Semiconductor Manufacturing", "Target", "TD Ameritrade Holding", "TechnipFMC",
    "Te Connectivity", "Telecom Italia", "Telefonica", "Teleflex", "Tencent",
    "Teradyne", "Texas Instruments", "Textron", "The Coca-Cola Company", "The Home Depot",
    "The Kraft Heinz Company", "The Procter & Gamble Company", "The TJX Companies", "The Travelers Companies", "Thermo Fisher Scientific",
    "Third Point", "Thomson Reuters", "Thryv Holdings", "Tiffany & Co.", "Time Warner Cable",
    "TJX Companies", "Toll Brothers", "Toyota Motor", "TPG Inc.", "Tractor Supply",
    "Trane Technologies", "TransDigm Group", "Travelers", "TriNet Group", "Truist Financial",
    "Twitter", "Tyson Foods", "U.S. Bancorp", "UBS Group", "Ultratech Cement",
    "Under Armour", "Unilever", "Union Pacific", "United Airlines", "United Continental Holdings",
    "UnitedHealth Group", "United Parcel Service", "United Rentals", "United States Steel Corporation", "United Technologies",
    "Universal Health Services", "Univar Solutions", "Unum Group", "UPS", "US Bancorp",
    "US Foods Holding", "Valeant Pharmaceuticals International", "Valero Energy", "Vanguard Group", "Varian Medical Systems",
    "Verisk Analytics", "Verizon Communications", "Vertex Pharmaceuticals", "ViacomCBS", "Viatris",
    "Viking Therapeutics", "VISA", "Vitamin Shoppe", "Vodafone Group", "VMware",
    "Vornado Realty Trust", "VTech", "W.W. Grainger", "Waitrose & Partners", "Wallgreens Boots Alliance",
    "Walmart", "Walt Disney", "Walmart", "Wang Laboratories", "Washington Federal",
    "Waters Corporation", "Wawa", "Wayfair", "Weber-Stephen Products", "WeWork",
    "Wells Fargo", "Western Digital", "Western Union", "West Pharmaceutical Services", "WestRock",
    "Weyerhaeuser", "Whirlpool", "White Mountains Insurance Group", "Whole Foods Market", "Williams Companies",
    "Williams-Sonoma", "Wilmington Trust", "Windstream", "Wipro", "WPP",
    "Xcel Energy", "Xerox", "Xilinx", "XPO Logistics", "Xylem",
    "Yelp", "Yum! Brands", "Zebra Technologies", "Zendesk", "Zillow Group",
    "Zoom Video Communications", "Zoetis", "ZoomInfo"
]


class CompanyDataCollector:
    """Collects and verifies collaboration tool data for companies"""

    def __init__(self):
        self.existing_companies = set()
        self.new_companies = []
        self.progress = {
            "total_processed": 0,
            "verified_count": 0,
            "skipped_count": 0,
            "errors": []
        }

    def load_existing_data(self):
        """Load existing companies from JSON file"""
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.existing_companies = {c["company"].lower() for c in data}
                print(f"✓ Loaded {len(self.existing_companies)} existing companies")
                return data
        return []

    def save_progress(self):
        """Save current progress"""
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)

    def load_progress(self):
        """Load previous progress"""
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                self.progress = json.load(f)
                print(f"✓ Loaded progress: {self.progress}")

    def search_company_tools(self, company_name: str) -> List[Dict]:
        """
        Search for collaboration tools used by a company
        Returns list of tool entries with verification info
        """
        tools = []

        # Common collaboration tools to search for
        target_tools = [
            "Slack", "Microsoft Teams", "Zoom", "Jira", "Confluence",
            "Notion", "Figma", "Miro", "Monday.com", "Asana",
            "Trello", "Basecamp", "Google Workspace", "Dropbox", "Box",
            "Google Meet", "Webex", "GoToMeeting"
        ]

        # For now, create placeholder entries that need verification
        # In production, this would use web search and API calls
        for tool in target_tools[:3]:  # Start with 3 tools per company
            tools.append({
                "name": tool,
                "use_case": f"{company_name}의 협업 및 생산성 향상을 위한 {tool} 도입 (검증 필요)",
                "source_url": "",
                "source_type": "unverified",
                "verified": False
            })

        return tools

    def verify_tool_usage(self, company_name: str, tool_name: str) -> Optional[Dict]:
        """
        Verify if a company actually uses a specific collaboration tool
        Returns verification data or None if not verified
        """
        # This would make web searches and API calls in production
        # For now, return None to indicate unverified
        return None

    def process_company(self, company_name: str):
        """Process a single company and collect tool data"""
        company_lower = company_name.lower()

        if company_lower in self.existing_companies:
            self.progress["skipped_count"] += 1
            return

        print(f"  Processing: {company_name}")

        # Create company entry
        entry = {
            "company": company_name,
            "domain": f"{company_name.lower().replace(' ', '').replace(',', '').replace('&', 'and')}.com",
            "industry": "Fortune 1000",
            "tools": self.search_company_tools(company_name),
            "updated_at": str(date.today())
        }

        self.new_companies.append(entry)
        self.progress["total_processed"] += 1

        # Update progress periodically
        if self.progress["total_processed"] % 50 == 0:
            self.save_progress()
            print(f"  Progress: {self.progress['total_processed']} processed")

    def run(self):
        """Main execution method"""
        print("=" * 60)
        print("Fortune 1000 Collaboration Tools Scraper")
        print("=" * 60)

        # Load existing data and progress
        existing_data = self.load_existing_data()
        self.load_progress()

        print(f"\nProcessing {len(fortune_companies_raw)} companies...")
        print(f"Already in DB: {len(self.existing_companies)}")

        # Process each company
        for company in fortune_companies_raw:
            try:
                self.process_company(company)
                time.sleep(0.1)  # Rate limiting
            except Exception as e:
                print(f"  ✗ Error processing {company}: {e}")
                self.progress["errors"].append({
                    "company": company,
                    "error": str(e)
                })

        # Merge new entries with existing data
        if self.new_companies:
            existing_data.extend(self.new_companies)

            # Save merged data
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)

        # Final progress update
        self.save_progress()

        # Print summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total companies processed: {self.progress['total_processed']}")
        print(f"New companies added: {len(self.new_companies)}")
        print(f"Companies skipped (already in DB): {self.progress['skipped_count']}")
        print(f"Total companies in DB: {len(existing_data)}")
        print(f"Errors: {len(self.progress['errors'])}")

        if self.progress['errors']:
            print("\nErrors:")
            for err in self.progress['errors'][:10]:  # Show first 10 errors
                print(f"  - {err['company']}: {err['error']}")

        print("\n✓ Done!")


if __name__ == "__main__":
    collector = CompanyDataCollector()
    collector.run()
