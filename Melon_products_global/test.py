#!/usr/bin/env python3
"""
fetch_raw_endprod.py
———————————————
Grabs the /main/ajax/endProdList.json payload for a single page,
writes the raw body (whatever the server returns – JSON or HTML fragment)
to ./endProdList_page{page}_size{size}.txt.

Requires: curl_cffi 0.6.3  ·  brotli
"""

import time, random, sys
from pathlib import Path
from urllib.parse import urlencode

from curl_cffi import requests     # HTTP/2 + Chrome JA3

# ------------------------------------------------------------------ #
# 0) CONFIG – clone from the working scraper
# ------------------------------------------------------------------ #
BASE   = "https://tkglobal.melon.com"
LANG   = "EN"
INDEX  = f"{BASE}/main/index.htm?langCd={LANG}"
UA     = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/138.0.0.0 Safari/537.36")

PROXY   = ""                       # fill in if needed
TIMEOUT = (15, 30)                 # (connect, read)

# ------------------------------------------------------------------ #
# 1) Build Chrome-like session (HTTP/2)
# ------------------------------------------------------------------ #
s = requests.Session()
s.headers.update({"User-Agent": UA})
s.http_versions = ["h2", "h3", "http/1.1"]
s.impersonate   = "chrome120"
if PROXY:
    s.proxies.update({"http": PROXY, "https": PROXY})
s.cookies.jar.persist_cookies = True

# ------------------------------------------------------------------ #
# 2) Header blocks (navigation & XHR)
# ------------------------------------------------------------------ #
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
    ("Accept", "application/json, text/javascript, */*; q=0.01"),
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

# ------------------------------------------------------------------ #
# 3) Warm-up  (sets WAF cookies)
# ------------------------------------------------------------------ #
def warm_up():
    r = s.get(INDEX, headers=NAV_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    print("Warm-up OK – cookies:",
          ", ".join(c.name for c in s.cookies))

# ------------------------------------------------------------------ #
# 4) Fetch and save raw body
# ------------------------------------------------------------------ #
def dump_raw(page: int = 1, size: int = 100, lang: str = LANG):
    params = {"langCd": lang, "pageIndex": str(page), "pgSize": str(size)}
    url    = f"{BASE}/main/ajax/endProdList.json?{urlencode(params)}"
    r      = s.get(url, headers=XHR_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    # filename = Path(f"endProdList_page{page}_size{size}.txt")
    filename = Path(f"raw.txt")

    filename.write_text(r.text, encoding="utf-8")
    print(f"Saved → {filename}  ({len(r.content):,} bytes)")

# ------------------------------------------------------------------ #
# 5) Entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    # Feel free to adjust these two values:
    PAGE_IDX  = 1
    PAGE_SIZE = 100000

    warm_up()
    dump_raw(PAGE_IDX, PAGE_SIZE)
