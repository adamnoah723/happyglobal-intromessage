"""
process_leads.py
Pulls lead data from a Google-Sheet-as-CSV link, scrapes each distributor
(homepage + up to 2 sub-pages), creates a 5–10-line profile and a highly
personalised first-touch email in a formal, approachable tone, and writes
enriched_results.csv (UTF-8 BOM) for easy Excel import.

ENV VARS (set as GitHub Secrets):
  OPENAI_KEY      – your OpenAI API key
  SHEET_CSV_URL   – the 'Publish to web' CSV link of your Google Sheet
MODEL:
  gpt-4o4-mini  # ← switch to "gpt-4o-mini" if you prefer the o3 model
"""

import os, re, time, random, io, requests, pandas as pd
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

OPENAI_KEY    = os.environ["OPENAI_KEY"]
SHEET_CSV_URL = os.environ["SHEET_CSV_URL"]

HEADERS = {"User-Agent": "Mozilla/5.0 Firefox/120.0"}

# ---------- Google-Sheet fetch ---------- #
def download_leads() -> pd.DataFrame:
    resp = requests.get(SHEET_CSV_URL, timeout=15)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))

# ---------- HTTP helpers ---------- #
def _get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=12)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def _extract_text(soup: BeautifulSoup) -> str:
    return soup.get_text(" ", strip=True).lower()

def _first_level_links(base_url: str, soup: BeautifulSoup, limit=2):
    root = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if not any(k in h.lower() for k in ("about", "service", "product", "contact")):
            continue
        full = h if h.startswith("http") else urljoin(root, h)
        if urlparse(full).netloc == urlparse(base_url).netloc and full not in seen:
            seen.add(full); out.append(full)
            if len(out) >= limit:
                break
    return out

# ---------- phone normaliser ---------- #
def _clean_phone(raw: str) -> str:
    if not raw: return ""
    raw = (raw.replace("\u2010", "-")
              .replace("\u2011", "-")
              .replace("\u2013", "-")
              .replace("\u2014", "-"))
    m = re.search(r"\(?(\d{3})\)?[\s\-]*(\d{3})[\s\-]*(\d{4})", raw)
    return f"({m.group(1)}) {m.group(2)}-{m.group(3)}" if m else raw

# ---------- scraper (homepage + 2 sub-pages) ---------- #
def scrape_site(url: str) -> dict:
    out = {"brief": "", "keywords": "", "phone": "", "error": ""}
    try:
        home = _get_soup(url)
    except Exception as e:
        out["error"] = f"homepage_error:{e}"
        return out

    meta = home.find("meta", attrs={"name": "description"})
    brief = meta["content"].strip() if meta else ""
    if not brief:
        p = home.find("p")
        brief = p.get_text(strip=True)[:250] if p else ""
    full_text = _extract_text(home)

    for link in _first_level_links(url, home):
        try:
            sub = _get_soup(link)
            full_text += " " + _extract_text(sub)
            time.sleep(random.uniform(1, 2))
        except Exception:
            continue

    kws = [w for w in (
        "convenience","organic","ethnic","asian","hispanic","natural","halal",
        "wholesale","foodservice","supermarket","c-store","grocery","distribution")
           if w in full_text]

    phone_match = re.search(r"\(\d{3}\)\s*\d{3}[-\u2010\u2011\u2013\u2014\s]\d{4}", full_text)
    out.update({
        "brief":     brief or "No meta description available.",
        "keywords":  ", ".join(sorted(set(kws))),
        "phone":     _clean_phone(phone_match.group(0)) if phone_match else ""
    })
    return out

# ---------- OpenAI helper ---------- #
def openai_chat(prompt: str) -> str:
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}",
                 "Content-Type": "application/json"},
        json={"model": "gpt-4o-mini",  # switch to "gpt-4o-mini" if desired
              "temperature": 0.6,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

# ---------- prompt builders ---------- #
def build_profile_prompt(company: str, brief: str, keywords: str) -> str:
    return (
        f"Write a concise 5–10 line profile of {company}. "
        f"Homepage description: {brief} "
        f"Keywords: {keywords or 'n/a'}. "
        "Highlight product categories, customer base, or geographic reach. "
        "Plain text only."
    )

def build_email(row, profile: str) -> str:
    contact  = row.get("ContactName", "") or "Snack Category Manager"
    greeting = f"Hello {contact},"
    intro    = ("My name is Adam Noah Azlan, Senior Business Development Representative at Happy Global, "
                "a U.S.–based importer of award-winning Asian snack brands.")

    # robust two-detail hook
    lines = [ln.strip() for ln in profile.splitlines() if ln.strip()]
    hook1 = lines[0] if lines else ""
    hook2 = lines[1] if len(lines) > 1 else ""
    hook = (f"We understand that {row['Company']} specialises in {hook1.lower()} "
            f"and {hook2.lower()}. Our snacks align directly with that focus.")

    products = (
        "• CRISUP Potato Sticks – freeze-dried then vacuum-fried (≈50 % less oil), "
        "zero trans fat, six gourmet flavours, #1 global potato-stick.\n"
        "• KOZED Peelable Gummies – 28 % real juice, Halal-certified, zero fat, interactive peelable fruit shapes."
    )
    incentives = ("MOQ tiers 10 / 70 (free shipping) / 140 cases; free merchandising strip per case; "
                  "one free branded display for every $500 ordered.")
    cta = ("Would a two-flavour tasting kit be helpful, or would you prefer a brief "
           "10-minute call to discuss next steps?")

    return (
        f"{greeting}\n\n"
        f"{intro}\n"
        f"{hook}\n\n"
        f"{products}\n\n"
        f"{incentives}\n\n"
        f"{cta}\n\n"
        "Best regards,\n"
        "Adam Noah Azlan\n"
        "Senior BD Representative · Happy Global\n"
        "+1 945-899-3624"
    )

# ---------- main ---------- #
def main():
    leads = download_leads()
    results = []

    for _, row in leads.iterrows():
        scraped  = scrape_site(row["Website"])
        profile  = openai_chat(build_profile_prompt(row["Company"], scraped["brief"], scraped["keywords"]))
        email    = openai_chat(build_email(row, profile))

        results.append({
            **row,
            "Phone":        scraped["phone"],
            "Profile":      profile,
            "TailoredEmail": email,
            "ScrapeError":  scraped["error"]
        })
        time.sleep(random.uniform(1, 2))

    pd.DataFrame(results).to_csv(
        "enriched_results.csv",
        index=False,
        encoding="utf-8-sig"  # UTF-8 with BOM for Excel/Sheets
    )

if __name__ == "__main__":
    main()
