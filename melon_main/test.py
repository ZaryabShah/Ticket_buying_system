#!/usr/bin/env python3
"""
concert_prods.impersonate   = "chrome138"
if PROXY:
    s.proxies.update({"http": PROXY, "https": PROXY})
s.cookies.jar.persist_cookies = True      # keep WAF cookies between calls_dump.py
————————————————————————
1. Loads the public concert index page once               →
   sets WAF cookies (PCID, _fwb, NetFunnel_ID, …)

2. Calls  /performance/ajax/prodList.json?sortType=HIT…   →
   receives ~1.4 MB of compressed JSON with every concert
   currently on Melon Ticket.

3. Writes the *raw* body to ./prodList_HIT.json

Requires :  curl_cffi >= 0.6.3   ·   brotli
            ( pip install curl_cffi brotli )
"""

import time, random, sys
from pathlib import Path
from urllib.parse import urlencode

from curl_cffi import requests      # HTTP/2 + Chrome-like TLS

# ────────────────────────────────────────────────────────────────
# CONFIG — copy-pasted from the working melon_global scraper
# ────────────────────────────────────────────────────────────────
BASE    = "https://ticket.melon.com"
INDEX   = f"{BASE}/concert/index.htm?genreType=GENRE_CON"
UA      = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 (KHTML, like Gecko) "
           "Chrome/138.0.0.0 Safari/537.36")

PROXY   = ""                 # "http://user:pass@host:port"  or leave blank
TIMEOUT = (15, 30)           # (connect, read)

# ────────────────────────────────────────────────────────────────
# 1) Build Chrome-like session (HTTP/2, header order, JA3)
# ────────────────────────────────────────────────────────────────
s = requests.Session()
s.headers.update({"User-Agent": UA})
s.http_versions = ["h2", "h3", "http/1.1"]
s.impersonate   = "chrome120"
if PROXY:
    s.proxies.update({"http": PROXY, "https": PROXY})
s.cookies.jar.persist_cookies = True      # keep WAF cookies between calls

# ────────────────────────────────────────────────────────────────
# 2) Header blocks exactly as DevTools shows
# ────────────────────────────────────────────────────────────────
NAV_HEADERS = [
    ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8,"
               "application/signed-exchange;v=b3;q=0.7"),
    ("Accept-Language", "en-US,en;q=0.9"),
    ("Accept-Encoding", "gzip, deflate, br, zstd"),
    ("Upgrade-Insecure-Requests", "1"),
    ("Sec-Fetch-Site", "none"),
    ("Sec-Fetch-Mode", "navigate"),
    ("Sec-Fetch-Dest", "document"),
    ("Sec-Fetch-User", "?1"),
]

XHR_HEADERS = [
    ("Accept", "*/*"),                       # the concert page really sends */*
    ("Accept-Language", "en-US,en;q=0.9"),
    ("Accept-Encoding", "gzip, deflate, br, zstd"),
    ("X-Requested-With", "XMLHttpRequest"),
    ("Sec-Fetch-Site", "same-origin"),
    ("Sec-Fetch-Mode", "cors"),
    ("Sec-Fetch-Dest", "empty"),
    ("Origin", BASE),
    ("Referer", INDEX),
    ("Sec-CH-UA", '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"'),
    ("Sec-CH-UA-Mobile", "?0"),
    ("Sec-CH-UA-Platform", '"Windows"'),
]

# ────────────────────────────────────────────────────────────────
# 3) Warm-up navigation  →  sets cookies needed for XHR
# ────────────────────────────────────────────────────────────────
def warm_up() -> None:
    r = s.get(INDEX, headers=NAV_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    print("✔ warm-up OK — cookies:",
          ", ".join(c.name for c in s.cookies.jar))

# ────────────────────────────────────────────────────────────────
# 4) Fetch prodList.json  (concert catalogue)
# ────────────────────────────────────────────────────────────────
def dump_prodlist(sort_type="HIT", file_name="prodList_HIT.json"):
    # identical to the browser’s query string
    params = {
        "commCode": "",
        "sortType": sort_type,
        "perfGenreCode": "GENRE_CON_ALL",
        "perfThemeCode": "",
        "filterCode": "FILTER_ALL",
        "v": "1",
    }
    url = f"{BASE}/performance/ajax/prodList.json?{urlencode(params)}"

    r = s.get(url, headers=XHR_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    Path(file_name).write_bytes(r.content)
    print(f"✔ saved → {file_name}  ({len(r.content):,} bytes)")

# ────────────────────────────────────────────────────────────────
# 5) Entry point
# ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    warm_up()
    # small pause to mimic human timing
    time.sleep(random.uniform(0.4, 0.6))
    dump_prodlist()
