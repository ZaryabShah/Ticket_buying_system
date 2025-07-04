#!/usr/bin/env python3
"""
melon_fetch_and_parse.py
Author: Assistant, 2025-07-04

Combined script that fetches Melon ticket data and immediately parses it into structured JSON.
No intermediate files - direct fetch-to-JSON processing.

Requires: curl_cffi 0.6.3  ·  brotli
"""

import json
import time
import random
import sys
from pathlib import Path
from urllib.parse import urlencode
from typing import Dict, List, Any, Optional

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
# 3) Data fetching functions
# ------------------------------------------------------------------ #
def warm_up():
    """Initialize session and set WAF cookies"""
    r = s.get(INDEX, headers=NAV_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    print("✔ Session warmed up – cookies:",
          ", ".join(c.name for c in s.cookies))

def fetch_melon_data(page: int = 1, size: int = 100, lang: str = LANG) -> Optional[Dict[str, Any]]:
    """Fetch data from Melon API and return parsed JSON"""
    try:
        params = {"langCd": lang, "pageIndex": str(page), "pgSize": str(size)}
        url    = f"{BASE}/main/ajax/endProdList.json?{urlencode(params)}"
        
        print(f"🌐 Fetching data from: {url}")
        r = s.get(url, headers=XHR_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        
        print(f"✔ Successfully fetched {len(r.content):,} bytes")
        
        # Parse JSON directly
        data = json.loads(r.text)
        print(f"✔ Successfully parsed JSON data")
        
        events = data.get('endProdList', [])
        print(f"  Found {len(events)} events")
        print(f"  Total count: {data.get('listCount', 'Unknown')}")
        
        return data
        
    except requests.RequestException as e:
        print(f"✗ Network error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON response: {e}")
        return None
    except Exception as e:
        print(f"✗ Error fetching data: {e}")
        return None

# ------------------------------------------------------------------ #
# 4) Data parsing functions
# ------------------------------------------------------------------ #
def extract_event_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and organize all available data from a single event"""
    
    # Get all available fields and organize them
    extracted_data = {}
    
    # Basic identification
    extracted_data['id'] = event.get('PROD_ID')
    
    # Titles in different languages
    extracted_data['titles'] = {
        'english': event.get('TITLE_EN'),
        'japanese': event.get('TITLE_JP'), 
        'chinese': event.get('TITLE_CN')
    }
    
    # Venue information
    extracted_data['venue'] = {
        'name_en': event.get('PLACE_HALL_NAME_EN'),
        'name_jp': event.get('PLACE_HALL_NAME_JP'),
        'name_cn': event.get('PLACE_HALL_NAME_CN')
    }
    
    # Performance type/category
    extracted_data['performance_type'] = {
        'english': event.get('PERF_TYPE_EN'),
        'japanese': event.get('PERF_TYPE_JP'),
        'chinese': event.get('PERF_TYPE_CN')
    }
    
    # Age restrictions
    extracted_data['age_grade'] = {
        'english': event.get('GRADE_EN'),
        'japanese': event.get('GRADE_JP'),
        'chinese': event.get('GRADE_CN')
    }
    
    # Dates and times
    extracted_data['schedule'] = {
        'period_info': event.get('PERIOD_INFO'),
        'start_date': event.get('PERF_START_DT'),
        'end_date': event.get('PERF_END_DT')
    }
    
    # Sales status
    extracted_data['sales_status'] = {
        'english': event.get('SELL_STATE_YN_EN') == 'Y',
        'japanese': event.get('SELL_STATE_YN_JP') == 'Y',
        'chinese': event.get('SELL_STATE_YN_CN') == 'Y'
    }
    
    # Images and media
    extracted_data['media'] = {
        'poster_image': event.get('POSTER_IMG')
    }
    
    # Additional metadata
    extracted_data['metadata'] = {
        'product_flag': event.get('PRODFLG'),
        'display_order': event.get('DISPLAY_ORDER_NO'),
        'registration_info': {
            'reg_user_id': event.get('REG_USER_ID'),
            'reg_date': event.get('REG_DATE'),
            'modified_user_id': event.get('MDF_USER_ID'),
            'modified_date': event.get('MDF_DATE')
        }
    }
    
    # Raw data (all original fields for reference)
    extracted_data['raw_data'] = event
    
    return extracted_data

def create_summary_stats(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create summary statistics from the parsed events"""
    
    stats = {
        'total_events': len(events),
        'performance_types': {},
        'venues': {},
        'language_availability': {
            'english': 0,
            'japanese': 0,
            'chinese': 0
        },
        'date_range': {
            'earliest_start': None,
            'latest_end': None
        }
    }
    
    start_dates = []
    end_dates = []
    
    for event in events:
        # Count performance types (English)
        perf_type = event.get('performance_type', {}).get('english')
        if perf_type:
            stats['performance_types'][perf_type] = stats['performance_types'].get(perf_type, 0) + 1
        
        # Count venues (English)
        venue = event.get('venue', {}).get('name_en')
        if venue:
            stats['venues'][venue] = stats['venues'].get(venue, 0) + 1
        
        # Count language availability
        sales_status = event.get('sales_status', {})
        if sales_status.get('english'):
            stats['language_availability']['english'] += 1
        if sales_status.get('japanese'):
            stats['language_availability']['japanese'] += 1
        if sales_status.get('chinese'):
            stats['language_availability']['chinese'] += 1
        
        # Collect dates
        start_date = event.get('schedule', {}).get('start_date')
        end_date = event.get('schedule', {}).get('end_date')
        if start_date and start_date != '99991231': 
            start_dates.append(start_date)
        if end_date and end_date != '99991231': 
            end_dates.append(end_date)
    
    # Calculate date range
    if start_dates:
        stats['date_range']['earliest_start'] = min(start_dates)
    if end_dates:
        stats['date_range']['latest_end'] = max(end_dates)
    
    return stats

def save_parsed_data(data: Dict[str, Any], events: List[Dict[str, Any]], output_file: str):
    """Save all parsed data to JSON file"""
    try:
        # Create comprehensive output structure
        output_data = {
            'metadata': {
                'parsing_timestamp': time.time(),
                'parsing_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'original_data': {
                    'list_count': data.get('listCount'),
                    'page_info': data.get('pageInfo', {}),
                    'result_code': data.get('resultCode'),
                    'result_message': data.get('resultMessage')
                }
            },
            'summary_statistics': create_summary_stats(events),
            'events': events,
            'raw_original_data': data  # Keep original for reference
        }
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✔ Data successfully saved to: {output_file}")
        print(f"  Total events processed: {len(events)}")
        print(f"  Performance types found: {len(output_data['summary_statistics']['performance_types'])}")
        print(f"  Venues found: {len(output_data['summary_statistics']['venues'])}")
        print(f"  File size: {Path(output_file).stat().st_size:,} bytes")
        
    except Exception as e:
        print(f"✗ Error saving data: {e}")

# ------------------------------------------------------------------ #
# 5) Main combined function
# ------------------------------------------------------------------ #
def fetch_and_parse_melon_data(page: int = 1, size: int = 100, output_file: str = None, lang: str = LANG):
    """
    Main function to fetch and parse Melon data in one step
    
    Args:
        page: Page index to fetch (default: 1)
        size: Number of events per page (default: 100)
        output_file: Output JSON file path (auto-generated if None)
        lang: Language code (default: EN)
    """
    
    print("=" * 60)
    print("MELON TICKET DATA FETCHER & PARSER")
    print("=" * 60)
    
    # Step 1: Initialize session
    print("\n🔧 Initializing session...")
    warm_up()
    
    # Step 2: Fetch data from Melon
    print(f"\n📡 Fetching data (page={page}, size={size}, lang={lang})...")
    parsed_data = fetch_melon_data(page, size, lang)
    if not parsed_data:
        print("✗ Failed to fetch data. Exiting.")
        return False
    
    # Step 3: Extract events
    events_raw = parsed_data.get('endProdList', [])
    if not events_raw:
        print("✗ No events found in the fetched data")
        return False
    
    print(f"\n🔍 Processing {len(events_raw)} events...")
    print("-" * 40)
    
    # Step 4: Process each event
    processed_events = []
    for i, event_raw in enumerate(events_raw):
        try:
            extracted_event = extract_event_data(event_raw)
            processed_events.append(extracted_event)
            
            # Show progress
            if (i + 1) % 10 == 0 or i == len(events_raw) - 1:
                print(f"  Processed {i + 1}/{len(events_raw)} events...")
                
        except Exception as e:
            print(f"⚠ Error processing event {i + 1}: {e}")
            # Add the raw event data even if processing failed
            processed_events.append({
                'error': str(e),
                'raw_data': event_raw
            })
    
    # Step 5: Generate output filename if not provided
    if not output_file:
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        output_file = f"melon_events_page{page}_size{size}_{timestamp}.json"
    
    # Step 6: Save all data
    save_parsed_data(parsed_data, processed_events, output_file)
    
    # Step 7: Print sample of extracted data
    if processed_events:
        print(f"\n📋 Sample of extracted fields from first event:")
        sample_event = processed_events[0]
        print(f"  ID: {sample_event.get('id')}")
        print(f"  Title (EN): {sample_event.get('titles', {}).get('english')}")
        print(f"  Performance Type: {sample_event.get('performance_type', {}).get('english')}")
        print(f"  Venue: {sample_event.get('venue', {}).get('name_en')}")
        print(f"  Period: {sample_event.get('schedule', {}).get('period_info')}")
    
    return True

# ------------------------------------------------------------------ #
# 6) Entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    
    # parser = argparse.ArgumentParser(
    #     description="Fetch and parse Melon ticket data into structured JSON"
    # )
    # parser.add_argument(
    #     '--page', '-p', 
    #     type=int, 
    #     default=1, 
    #     help='Page index to fetch (default: 1)'
    # )
    # parser.add_argument(
    #     '--size', '-s', 
    #     type=int, 
    #     default=100, 
    #     help='Number of events per page (default: 100)'
    # )
    # parser.add_argument(
    #     '--output', '-o', 
    #     type=str, 
    #     default=None, 
    #     help='Output JSON file path (auto-generated if not specified)'
    # )
    # parser.add_argument(
    #     '--lang', '-l', 
    #     type=str, 
    #     default='EN', 
    #     choices=['EN', 'JP', 'CN'],
    #     help='Language code (default: EN)'
    # )
    
    # args = parser.parse_args()
    
    # Run the combined fetch and parse operation
    success = fetch_and_parse_melon_data(
        page= 1,
        size=100000,
        output_file="output.json",  # Auto-generated if None
        lang='EN'
    )
    
    if success:
        print("\n🎉 Fetch and parse operation completed successfully!")
    else:
        print("\n❌ Fetch and parse operation failed!")
        sys.exit(1)
