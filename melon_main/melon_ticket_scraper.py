#!/usr/bin/env python3
"""
Melon Ticket Comprehensive Scraper & Parser
==========================================

A complete solution that:
1. Fetches event data from multiple Melon Ticket categories
2. Parses and structures the JSON data 
3. Provides detailed statistics and summaries
4. Saves both raw and parsed data for each category

Categories:
- Concerts (GENRE_CON_ALL)
- Arts/Theater (GENRE_ART_ALL) 
- Fan meetings (GENRE_FAN_ALL)
- Classical (GENRE_CLA_ALL)
- Exhibitions (GENRE_EXH_ALL)
- All genres (GENRE_ALL)

Requirements: curl_cffi >= 0.6.3, brotli
"""

import time
import random
import sys
import json
import glob
from pathlib import Path
from urllib.parse import urlencode
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import Counter

from curl_cffi import requests

# ────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────
BASE = "https://ticket.melon.com"
INDEX = f"{BASE}/concert/index.htm?genreType=GENRE_CON"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/138.0.0.0 Safari/537.36")

PROXY = ""  # "http://user:pass@host:port" or leave blank
TIMEOUT = (15, 30)  # (connect, read)

# Categories to fetch
CATEGORIES = {
    "concerts": {
        "name": "Concerts", 
        "perfGenreCode": "GENRE_CON_ALL",
        "perfThemeCode": "",
        "description": "All concert events"
    },
    "arts": {
        "name": "Arts & Theater",
        "perfGenreCode": "GENRE_ART_ALL", 
        "perfThemeCode": "",
        "description": "Theater, musicals, and art performances"
    },
    "fanmeetings": {
        "name": "Fan Meetings",
        "perfGenreCode": "GENRE_FAN_ALL",
        "perfThemeCode": "", 
        "description": "Fan meetings and special events"
    },
    "classical": {
        "name": "Classical",
        "perfGenreCode": "GENRE_CLA_ALL",
        "perfThemeCode": "",
        "description": "Classical music and opera"
    },
    "exhibitions": {
        "name": "Exhibitions", 
        "perfGenreCode": "GENRE_EXH_ALL",
        "perfThemeCode": "",
        "description": "Exhibitions and cultural events"
    },
    "all": {
        "name": "All Categories",
        "perfGenreCode": "GENRE_ALL",
        "perfThemeCode": "THEME_ALL",
        "description": "All available events across genres"
    }
}

# ────────────────────────────────────────────────────────────────
# HTTP Session Setup
# ────────────────────────────────────────────────────────────────
def create_session():
    """Create and configure a Chrome-like session"""
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    s.http_versions = ["h2", "h3", "http/1.1"]
    s.impersonate = "chrome120"
    if PROXY:
        s.proxies.update({"http": PROXY, "https": PROXY})
    s.cookies.jar.persist_cookies = True
    return s

# Header configurations
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
    ("Accept", "*/*"),
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
# Parsing Utilities
# ────────────────────────────────────────────────────────────────
NESTED_JSON_FIELDS = {
    "seatGradeJson",
    "saleTypeJson", 
    "perfRelatJson",
}

def _safe_json_load(raw: str | dict | list | None) -> Any:
    """Return parsed JSON if raw is a JSON string, else return raw unchanged."""
    if not isinstance(raw, str):
        return raw
    raw = raw.strip()
    if not raw:
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw

def _decode_nested(event: Dict[str, Any]) -> Dict[str, Any]:
    """Decode known nested JSON columns in-place and return event."""
    for field in NESTED_JSON_FIELDS:
        if field in event:
            event[field] = _safe_json_load(event[field])
    return event

def _attach_lists(event: Dict[str, Any]) -> None:
    """Add seatGrades and saleTypes lists derived from nested objects."""
    # Seat grades
    seat_grades: List[Dict[str, Any]] = []
    sg_root = event.get("seatGradeJson")
    if isinstance(sg_root, dict):
        for item in sg_root.get("data", {}).get("list", []):
            seat_grades.append(item)
    event["seatGrades"] = seat_grades

    # Sale types
    sale_types: List[Dict[str, Any]] = []
    st_root = event.get("saleTypeJson")
    if isinstance(st_root, dict):
        for poc in st_root.get("data", {}).get("list", []):
            base = {
                "pocName": poc.get("pocName"),
                "pocCode": poc.get("pocCode"),
            }
            for st in poc.get("saleTypeCodeList", []):
                sale_types.append({**base, **st})
    event["saleTypes"] = sale_types

    # Performance relations
    pr_root = event.get("perfRelatJson")
    if isinstance(pr_root, dict):
        event["perfRelat"] = pr_root.get("data", {}).get("list", [])
    else:
        event["perfRelat"] = []

# ────────────────────────────────────────────────────────────────
# Data Fetching
# ────────────────────────────────────────────────────────────────
def warm_up_session(session):
    """Warm up session by visiting index page to set cookies"""
    print("🔄 Warming up session...")
    r = session.get(INDEX, headers=NAV_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    print(f"✅ Session warmed up - cookies: {', '.join(c.name for c in session.cookies.jar)}")

def fetch_category_data(session, category_key: str, category_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Fetch data for a specific category"""
    print(f"\n🎭 Fetching {category_info['name']} ({category_key})...")
    
    params = {
        "commCode": "",
        "sortType": "HIT",
        "perfGenreCode": category_info["perfGenreCode"],
        "perfThemeCode": category_info["perfThemeCode"],
        "filterCode": "FILTER_ALL",
        "v": "1",
    }
    
    url = f"{BASE}/performance/ajax/prodList.json?{urlencode(params)}"
    
    try:
        r = session.get(url, headers=XHR_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        
        # Save raw data
        raw_filename = f"melon_{category_key}_raw.json"
        Path(raw_filename).write_bytes(r.content)
        print(f"💾 Raw data saved: {raw_filename} ({len(r.content):,} bytes)")
        
        # Parse JSON
        data = json.loads(r.text)
        return data
        
    except Exception as e:
        print(f"❌ Error fetching {category_key}: {e}")
        return None

def parse_event_data(raw_data: Dict[str, Any], category_key: str, category_info: Dict[str, str]) -> Dict[str, Any]:
    """Parse and structure event data"""
    print(f"🔧 Parsing {category_info['name']} data...")
    
    raw_events: List[Dict[str, Any]] = raw_data.get("data", [])
    events: List[Dict[str, Any]] = []

    for ev in raw_events:
        if not ev:  # Skip empty events
            continue
        ev = _decode_nested(ev)
        _attach_lists(ev)
        events.append(ev)

    parsed_data = {
        "category": category_key,
        "category_name": category_info["name"],
        "description": category_info["description"],
        "source_url_params": {
            "perfGenreCode": category_info["perfGenreCode"],
            "perfThemeCode": category_info["perfThemeCode"]
        },
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "total_events": len(events),
        "events": events
    }
    
    # Save parsed data
    parsed_filename = f"melon_{category_key}_parsed.json"
    with open(parsed_filename, "w", encoding="utf-8") as f:
        json.dump(parsed_data, f, ensure_ascii=False, indent=2)
    print(f"💾 Parsed data saved: {parsed_filename}")
    
    return parsed_data

# ────────────────────────────────────────────────────────────────
# Statistics & Analysis
# ────────────────────────────────────────────────────────────────
def calculate_category_stats(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate detailed statistics for a category"""
    events = parsed_data.get("events", [])
    
    if not events:
        return {"total_events": 0}
    
    # Basic counts
    total_events = len(events)
    
    # Venue analysis
    venues = [event.get("placeName", "Unknown") for event in events if event.get("placeName")]
    venue_counts = Counter(venues)
    unique_venues = len(venue_counts)
    
    # Region analysis  
    regions = [event.get("regionName", "Unknown") for event in events if event.get("regionName")]
    region_counts = Counter(regions)
    
    # Date analysis
    dates = []
    for event in events:
        period_info = event.get("periodInfo", "")
        if period_info:
            dates.append(period_info)
    
    # Price analysis
    prices = []
    for event in events:
        for seat_grade in event.get("seatGrades", []):
            price = seat_grade.get("basePrice")
            if price and isinstance(price, (int, float)):
                prices.append(price)
    
    price_stats = {}
    if prices:
        price_stats = {
            "min_price": min(prices),
            "max_price": max(prices),
            "avg_price": round(sum(prices) / len(prices), 2),
            "total_price_points": len(prices)
        }
    
    # Genre/Type analysis
    genre_codes = [event.get("perfTypeCode", "Unknown") for event in events if event.get("perfTypeCode")]
    genre_counts = Counter(genre_codes)
    
    return {
        "total_events": total_events,
        "venues": {
            "total_unique_venues": unique_venues,
            "top_venues": dict(venue_counts.most_common(10)),
            "venue_distribution": dict(venue_counts)
        },
        "regions": {
            "total_regions": len(region_counts),
            "region_distribution": dict(region_counts.most_common())
        },
        "pricing": price_stats,
        "event_types": {
            "type_distribution": dict(genre_counts)
        },
        "date_range": {
            "total_date_entries": len(dates),
            "sample_dates": dates[:5] if dates else []
        }
    }

def generate_comprehensive_summary(all_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Generate a comprehensive summary across all categories"""
    print("\n📊 Generating comprehensive summary...")
    
    total_events = 0
    all_venues = set()
    all_regions = set()
    all_prices = []
    category_summaries = {}
    
    for category_key, data in all_data.items():
        if not data:
            continue
            
        stats = calculate_category_stats(data)
        category_summaries[category_key] = {
            "name": data.get("category_name", category_key),
            "total_events": stats.get("total_events", 0),
            "unique_venues": stats.get("venues", {}).get("total_unique_venues", 0),
            "regions": list(stats.get("regions", {}).get("region_distribution", {}).keys()),
            "top_venues": list(stats.get("venues", {}).get("top_venues", {}).keys())[:5]
        }
        
        total_events += stats.get("total_events", 0)
        
        # Collect venues
        venue_dist = stats.get("venues", {}).get("venue_distribution", {})
        all_venues.update(venue_dist.keys())
        
        # Collect regions
        region_dist = stats.get("regions", {}).get("region_distribution", {})
        all_regions.update(region_dist.keys())
        
        # Collect prices
        pricing = stats.get("pricing", {})
        if "min_price" in pricing:
            all_prices.extend([pricing["min_price"], pricing["max_price"]])
    
    # Overall statistics
    overall_stats = {
        "total_events_across_categories": total_events,
        "total_unique_venues": len(all_venues),
        "total_regions": len(all_regions),
        "venue_list": sorted(list(all_venues)),
        "region_list": sorted(list(all_regions)),
        "category_breakdown": category_summaries,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    
    if all_prices:
        overall_stats["price_range"] = {
            "lowest_price": min(all_prices),
            "highest_price": max(all_prices)
        }
    
    return overall_stats

def print_summary_report(summary: Dict[str, Any]):
    """Print a formatted summary report"""
    print("\n" + "="*80)
    print("🎭 MELON TICKET COMPREHENSIVE SCRAPING SUMMARY")
    print("="*80)
    
    print(f"📅 Generated: {summary.get('generated_at', 'Unknown')}")
    print(f"🎪 Total Events: {summary.get('total_events_across_categories', 0):,}")
    print(f"🏛️  Total Venues: {summary.get('total_unique_venues', 0):,}")
    print(f"🌏 Total Regions: {summary.get('total_regions', 0):,}")
    
    price_range = summary.get('price_range', {})
    if price_range:
        print(f"💰 Price Range: ₩{price_range.get('lowest_price', 0):,} - ₩{price_range.get('highest_price', 0):,}")
    
    print("\n📋 CATEGORY BREAKDOWN:")
    print("-" * 50)
    
    for cat_key, cat_data in summary.get('category_breakdown', {}).items():
        print(f"\n🎭 {cat_data.get('name', cat_key).upper()}")
        print(f"   Events: {cat_data.get('total_events', 0):,}")
        print(f"   Venues: {cat_data.get('unique_venues', 0):,}")
        print(f"   Regions: {', '.join(cat_data.get('regions', [])[:3])}...")
        if cat_data.get('top_venues'):
            print(f"   Top Venues: {', '.join(cat_data['top_venues'])}")
    
    print("\n🏛️  TOP VENUES (All Categories):")
    print("-" * 30)
    venues = summary.get('venue_list', [])
    for i, venue in enumerate(venues[:15], 1):
        print(f"{i:2d}. {venue}")
    
    if len(venues) > 15:
        print(f"    ... and {len(venues) - 15} more venues")
    
    print("\n🌏 REGIONS:")
    print("-" * 15)
    regions = summary.get('region_list', [])
    print(", ".join(regions))
    
    print("\n" + "="*80)

# ────────────────────────────────────────────────────────────────
# Main Execution
# ────────────────────────────────────────────────────────────────
def main():
    """Main execution function"""
    print("🚀 Starting Melon Ticket Comprehensive Scraper")
    print(f"📊 Will fetch {len(CATEGORIES)} categories")
    
    session = create_session()
    warm_up_session(session)
    
    all_category_data = {}
    
    # Fetch and parse each category
    for category_key, category_info in CATEGORIES.items():
        # Add delay between requests
        if len(all_category_data) > 0:
            delay = random.uniform(1.0, 2.0)
            print(f"⏱️  Waiting {delay:.1f}s before next request...")
            time.sleep(delay)
        
        # Fetch raw data
        raw_data = fetch_category_data(session, category_key, category_info)
        if not raw_data:
            continue
        
        # Parse and structure data
        parsed_data = parse_event_data(raw_data, category_key, category_info)
        all_category_data[category_key] = parsed_data
        
        # Quick stats for this category
        stats = calculate_category_stats(parsed_data)
        print(f"📈 {category_info['name']}: {stats.get('total_events', 0)} events, "
              f"{stats.get('venues', {}).get('total_unique_venues', 0)} venues")
    
    # Generate comprehensive summary
    if all_category_data:
        summary = generate_comprehensive_summary(all_category_data)
        
        # Save summary
        summary_filename = "melon_ticket_comprehensive_summary.json"
        with open(summary_filename, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Comprehensive summary saved: {summary_filename}")
        
        # Print report
        print_summary_report(summary)
        
        print(f"\n🎉 Scraping completed successfully!")
        print(f"📁 Files generated:")
        for category_key in all_category_data.keys():
            print(f"   • melon_{category_key}_raw.json")
            print(f"   • melon_{category_key}_parsed.json")
        print(f"   • {summary_filename}")
    
    else:
        print("❌ No data was successfully fetched from any category")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
