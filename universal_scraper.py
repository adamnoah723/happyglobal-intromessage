import csv, re, time, random, requests, pandas as pd
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; rv:120.0) Gecko/20100101 Firefox/120.0"
}

def scrape_site(url):
    data = {"url": url}
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
    except Exception as e:
        data["error"] = str(e)
        return data

    soup = BeautifulSoup(r.text, "html.parser")
    # Short description
    meta = soup.find("meta", attrs={"name": "description"})
    brief = meta["content"].strip() if meta else ""
    if not brief:
        p = soup.find("p")
        brief = p.get_text(strip=True)[:250] if p else ""
    text = soup.get_text(" ", strip=True).lower()

    # Helpers
    def find(pattern):
        m = re.search(pattern, text)
        return m.group(0) if m else ""

    data.update({
        "brief": brief,
        "email_found": find(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}"),
        "phone_found": find(r"\(\d{3}\)\s*\d{3}[-\s]\d{4}"),
        "location_guess": find(r"[a-zA-Z ]+, CA"),
        "categories": ", ".join([w for w in ["convenience","organic","ethnic","asian","hispanic"] if w in text])
    })
    return data

def main():
    with open("urls.csv", newline="") as f:
        urls = [row[0] for row in csv.reader(f)]
    results = []
    for u in urls:
        results.append(scrape_site(u))
        time.sleep(random.uniform(1,2))    # polite delay
    pd.DataFrame(results).to_csv("results.csv", index=False)

if __name__ == "__main__":
    main()
