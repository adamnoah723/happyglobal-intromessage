# process_leads.py
"""
Pull lead data from a published Google Sheet (CSV),
scrape each distributor site (homepage + 1-2 key sub-pages),
create a concise profile and tailored email,
and output enriched_results.csv for later review.

Environment variables required (set in GitHub Actions secrets):
  OPENAI_KEY     – your OpenAI API key
  SHEET_CSV_URL  – the 'publish-to-web' CSV link of your lead sheet
"""

import os, re, time, random, io, requests, pandas as pd
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# ----- Environment ----- #
OPENAI_KEY    = os.environ["OPENAI_KEY"]
SHEET_CSV_URL = os.environ["SHEET_CSV_URL"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; rv:120.0) Gecko/20100101 Firefox/120.0"
}

# ----- Helper: fetch sheet data ----- #
def download_leads() -> pd.DataFrame:
    """CSV must have headers: Company, Website, ContactName, ContactEmail, Location."""
    resp = requests.get(SHEET_CSV_URL, timeout=15)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))

# ----- Helper: HTTP + parsing ----- #
def _get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=12)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def _extract_text(soup: BeautifulSoup) -> str:
    return soup.get_text(" ", strip=True).lower()

def _first_level_links(base_url: str, soup: BeautifulSoup, limit: int = 2):
    """Return up to `limit` internal links likely to be about/contact/product pages."""
    base_root = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
    seen, good = set(), []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not any(k in href.lower() for k in ("about", "service", "product", "contact")):
            continue
        full = href if href.startswith("http") else urljoin(base_root, href)
        if urlparse(full).netloc == urlparse(base_url).netloc and full not in seen:
            seen.add(full)
            good.append(full)
        if len(good) >= limit:
            break
    return good

# ----- Scraper: homepage + shallow crawl ----- #
def scrape_site(url: str) -> dict:
    data = {"brief": "", "keywords": "", "error": ""}
    try:
        home = _get_soup(url)
    except Exception as e:
        data["error"] = f"homepage_error:{str(e)[:60]}"
        return data

    # meta description or first paragraph
    meta = home.find("meta", attrs={"name": "description"})
    brief = meta["content"].strip() if meta else ""
    if not brief:
        p = home.find("p")
        brief = p.get_text(strip=True)[:250] if p else ""
    full_text = _extract_text(home)

    # crawl 1–2 additional pages
    for link in _first_level_links(url, home, limit=2):
        try:
            sub = _get_soup(link)
            full_text += " " + _extract_text(sub)
            time.sleep(random.uniform(1, 2))      # polite delay
        except Exception:
            continue

    keywords = [w for w in (
        "convenience", "organic", "ethnic", "asian", "hispanic",
        "natural", "halal", "wholesale", "foodservice", "supermarket",
        "c-store", "grocery", "distribution"
    ) if w in full_text]

    data["brief"]    = brief or "No meta description available."
    data["keywords"] = ", ".join(sorted(set(keywords)))
    return data

# ----- OpenAI chat helper ----- #
def openai_chat(prompt: str, model: str = "gpt-3.5-turbo") -> str:
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

# ----- Prompt builders ----- #
def build_profile_prompt(company: str, brief: str, keywords: str) -> str:
    return (
        f"Write a concise 5–10 line profile of {company}. "
        f"Homepage description: {brief} "
        f"Keywords: {keywords or 'n/a'}. "
        "Highlight product categories, customer base, geographic reach, and anything relevant for a snack supplier. "
        "Plain text only."
    )

def build_email_prompt(row, profile: str) -> str:
    return f"""
You are Adam Noah Azlan, Senior Business Development Rep at Happy Global.

Write a ~120-word first-touch sales email to {row.ContactName or 'the snack buyer'} at {row.Company} in {row.Location or 'California'}.

Company profile (for personalisation):
{profile}

Happy Global products:
• CRISUP potato sticks – freeze-dried & vacuum-fried, zero trans fat, six flavours.
• KOZED peelable halal gummies – 28% real juice, zero fat.

Incentives:
• MOQ tiers: 10 / 70 (free shipping) / 140 cases.
• Free merchandising strip per case.
• One free display for every $500 ordered.
• Sample case on request.

Email requirements:
• 1-sentence hook referencing their business.
• Offer samples and mention free shipping ≥70 cases.
• Ask for preferred ship-to address or a quick call.
• Friendly, professional tone. Do not exceed 130 words."""
# ----- Main flow ----- #
def main():
    leads = download_leads()
    results = []

    for _, row in leads.iterrows():
        scrape = scrape_site(row["Website"])
        profile = openai_chat(
            build_profile_prompt(row["Company"], scrape["brief"], scrape["keywords"])
        )
        email = openai_chat(build_email_prompt(row, profile))

        results.append({
            **row,
            "Profile": profile,
            "TailoredEmail": email,
            "ScrapeError": scrape["error"]
        })
        time.sleep(random.uniform(1, 2))  # respect rate limits

    pd.DataFrame(results).to_csv("enriched_results.csv", index=False)

if __name__ == "__main__":
    main()
