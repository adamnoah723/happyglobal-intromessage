# process_leads.py
"""
Fetch lead URLs from a Google-Sheet-as-CSV, scrape each site,
generate a 5–10 line profile + tailored outreach email,
and save everything to enriched_results.csv.
"""

import os, re, time, random, io, csv, requests, pandas as pd
from bs4 import BeautifulSoup

# ★★  CHANGE THIS to your published-CSV link (include https://…output=csv)  ★★
SHEET_CSV_URL = os.environ["SHEET_CSV_URL"]     # pulled from GitHub secret

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; rv:120.0) Gecko/20100101 Firefox/120.0"
}
OPENAI_KEY = os.environ["OPENAI_KEY"]

# ---------- Helper functions ---------- #
def download_leads():
    """Return DataFrame with columns Company, Website, ContactName, ContactEmail, Location"""
    resp = requests.get(SHEET_CSV_URL, timeout=15)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))

def scrape_site(url: str) -> dict:
    """Lightweight homepage scraper."""
    data = {"brief": "", "keywords": "", "error": ""}
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
    except Exception as e:
        data["error"] = f"fetch_fail: {e}"
        return data

    soup = BeautifulSoup(r.text, "html.parser")
    meta = soup.find("meta", attrs={"name": "description"})
    brief = meta["content"].strip() if meta else ""
    if not brief:
        p = soup.find("p")
        brief = p.get_text(strip=True)[:250] if p else ""
    text = soup.get_text(" ", strip=True).lower()

    kws = [w for w in ["convenience", "organic", "ethnic", "asian",
                       "hispanic", "natural", "halal", "wholesale"] if w in text]
    data["brief"] = brief or "No meta description available."
    data["keywords"] = ", ".join(kws)
    return data

def openai_chat(prompt: str, model="gpt-3.5-turbo") -> str:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_KEY}",
        "Content-Type": "application/json"
    }
    body = {
        "model": model,
        "temperature": 0.7,
        "messages": [{"role": "user", "content": prompt}]
    }
    r = requests.post(url, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

def profile_prompt(company: str, brief: str, keywords: str) -> str:
    return (f"Write a concise 5–10-line profile of {company}. "
            f"Homepage description: {brief} "
            f"Keywords: {keywords or 'n/a'}. "
            "Highlight product categories, customer base, or geographic reach if implied. "
            "Plain text, no bullet symbols.")

def email_prompt(row, profile: str) -> str:
    return f"""
You are Adam Noah Azlan, Senior Business Development Rep at Happy Global.

Write a warm ~120-word first-touch sales email to {row.ContactName or 'the snack buyer'} at {row.Company} in {row.Location or 'California'}.

Profile notes: {profile}

Happy Global products:
• CRISUP potato sticks – freeze-dried & vacuum-fried, 0 trans fat, six flavours.
• KOZED peelable halal gummies – 28% real juice, zero fat.

Incentives:
• Free shipping ≥70 cases.
• MOQ tiers: 10, 70, 140 (mix & match).
• Free merchandising strip per case.
• Free display per $500 ordered.
• Sample case on request.

Requirements:
• Personal opening hook referencing their business or keywords.
• Offer samples & mention free-shipping tier.
• Ask for preferred ship-to address or a quick call.
• Friendly professional tone.
"""

# ---------- Main flow ---------- #
def main():
    leads = download_leads()
    results = []

    for _, row in leads.iterrows():
        scrape = scrape_site(row["Website"])
        profile = openai_chat(profile_prompt(row["Company"], scrape["brief"], scrape["keywords"]))
        email   = openai_chat(email_prompt(row, profile))

        results.append({
            **row,
            "Profile": profile,
            "TailoredEmail": email,
            "ScrapeError": scrape["error"]
        })
        time.sleep(random.uniform(1, 2))  # polite delay

    pd.DataFrame(results).to_csv("enriched_results.csv", index=False)

if __name__ == "__main__":
    main()
