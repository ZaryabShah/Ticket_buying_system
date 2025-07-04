#!/usr/bin/env python3
"""
yes24_fetch_and_parse.py
Author: Assistant, 2025-07-04

Combined script that fetches Yes24 ticket data and immediately parses it into structured JSON.
No intermediate files - direct fetch-to-JSON processing.

Requires: requests, beautifulsoup4
"""

import json
import time
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode
import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------------ #
# 0) CONFIG
# ------------------------------------------------------------------ #
BASE_URL = "https://ticket.yes24.com"
DEFAULT_ENDPOINT = "/Pages/English/Perf/FnPerfList.aspx"
TIMEOUT = (15, 30)  # (connect, read)

# Genre mappings
GENRES = {
    'all': '',
    'concert': '15456',
    'musical': '15457', 
    'play': '15458',
    'exhibition': '15460',
    'classical': '15459'
}

# ------------------------------------------------------------------ #
# 1) Data fetching functions
# ------------------------------------------------------------------ #
def fetch_yes24_data(genre: str = 'concert') -> Optional[str]:
    """Fetch data from Yes24 and return HTML content"""
    try:
        # Setup session with proper headers
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Build URL with genre parameter
        params = {}
        if genre in GENRES and GENRES[genre]:
            params['Genre'] = GENRES[genre]
        
        url = f"{BASE_URL}{DEFAULT_ENDPOINT}"
        if params:
            url += '?' + urlencode(params)
        
        print(f"🌐 Fetching data from: {url}")
        
        # Make the request
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        
        print(f"✔ Successfully fetched {len(response.content):,} bytes")
        
        return response.text
        
    except requests.RequestException as e:
        print(f"✗ Network error: {e}")
        return None
    except Exception as e:
        print(f"✗ Error fetching data: {e}")
        return None

# ------------------------------------------------------------------ #
# 2) Data parsing functions (from yes24_html_parser.py)
# ------------------------------------------------------------------ #
def extract_event_id(url: str) -> Optional[str]:
    """Extract event ID from Yes24 URL"""
    try:
        match = re.search(r'IdPerf=(\d+)', url)
        if match:
            return match.group(1)
        return None
    except:
        return None

def extract_event_data(event_element) -> Dict[str, Any]:
    """Extract and organize all available data from a single event element"""
    
    extracted_data = {}
    
    try:
        # Find poster image and link
        poster_link = event_element.find('li', class_='poster')
        if poster_link:
            link_tag = poster_link.find('a')
            if link_tag:
                # Extract event URL and ID
                event_url = link_tag.get('href', '')
                extracted_data['event_url'] = event_url
                extracted_data['event_id'] = extract_event_id(event_url)
                
                # Extract poster image
                img_tag = link_tag.find('img')
                if img_tag:
                    extracted_data['poster_image'] = img_tag.get('src', '')
        
        # Find content details
        content_list = event_element.find('li', class_='conlist')
        if content_list:
            # Extract title
            title_tag = content_list.find('h3')
            if title_tag:
                title_link = title_tag.find('a')
                if title_link:
                    extracted_data['title'] = title_link.get_text(strip=True)
                else:
                    extracted_data['title'] = title_tag.get_text(strip=True)
            
            # Extract details from con_txt list
            details_list = content_list.find('ul', class_='con_txt')
            if details_list:
                details = {}
                for li in details_list.find_all('li'):
                    text = li.get_text(strip=True)
                    # Parse each detail line
                    if ':' in text:
                        key, value = text.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Clean up the key and map to standard names
                        if 'Genre' in key:
                            details['genre'] = value
                        elif 'Date/Time' in key:  # Exact match for "Date/Time"
                            details['date_time'] = value
                        elif 'Venue' in key:
                            details['venue'] = value
                        elif 'Age' in key:
                            details['age_group'] = value
                        elif key.strip() == 'Time':  # Exact match for "Time" (duration)
                            details['duration'] = value
                        else:
                            # Keep any other fields as-is
                            clean_key = key.lower().replace(' ', '_').replace('/', '_')
                            details[clean_key] = value
                
                extracted_data['details'] = details
        
        # Look for booking button/link
        booking_div = event_element.find_next_sibling('div', class_='btn')
        if booking_div:
            booking_link = booking_div.find('a')
            if booking_link:
                extracted_data['booking_url'] = booking_link.get('href', '')
        
        # Add parsing metadata
        extracted_data['metadata'] = {
            'parsed_timestamp': time.time(),
            'parsed_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source': 'yes24_fetch_and_parse'
        }
        
    except Exception as e:
        print(f"⚠ Error parsing event element: {e}")
        extracted_data['parsing_error'] = str(e)
    
    return extracted_data

def parse_yes24_html(html_content: str) -> List[Dict[str, Any]]:
    """Parse Yes24 HTML content and extract all events"""
    
    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        print("✔ Successfully parsed HTML content")
        
        # Find all event containers
        event_containers = soup.find_all('ul', class_='list_wrap')
        print(f"  Found {len(event_containers)} event containers")
        
        if not event_containers:
            print("✗ No event containers found in HTML")
            return []
        
        # Extract data from each event
        events = []
        for i, container in enumerate(event_containers):
            try:
                event_data = extract_event_data(container)
                if event_data:  # Only add if we got some data
                    events.append(event_data)
                    
                # Show progress
                if (i + 1) % 5 == 0 or i == len(event_containers) - 1:
                    print(f"  Processed {i + 1}/{len(event_containers)} events...")
                    
            except Exception as e:
                print(f"⚠ Error processing event {i + 1}: {e}")
                # Add error event for debugging
                events.append({
                    'parsing_error': str(e),
                    'event_index': i + 1,
                    'raw_html': str(container)[:200] + "..."
                })
        
        print(f"✔ Successfully extracted {len(events)} events")
        return events
        
    except Exception as e:
        print(f"✗ Error parsing HTML: {e}")
        return []

def create_summary_stats(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create summary statistics from the parsed events"""
    
    stats = {
        'total_events': len(events),
        'genres': {},
        'venues': {},
        'age_groups': {},
        'date_range': {
            'events_by_month': {}
        },
        'errors': 0
    }
    
    for event in events:
        # Count errors
        if 'parsing_error' in event:
            stats['errors'] += 1
            continue
            
        details = event.get('details', {})
        
        # Count genres
        genre = details.get('genre', 'Unknown')
        if genre:
            # Clean up genre (remove brackets)
            clean_genre = genre.replace('[', '').replace(']', '')
            stats['genres'][clean_genre] = stats['genres'].get(clean_genre, 0) + 1
        
        # Count venues
        venue = details.get('venue', 'Unknown')
        if venue:
            stats['venues'][venue] = stats['venues'].get(venue, 0) + 1
        
        # Count age groups
        age_group = details.get('age_group', 'Unknown')
        if age_group:
            stats['age_groups'][age_group] = stats['age_groups'].get(age_group, 0) + 1
        
        # Analyze dates
        date_time = details.get('date_time', '')
        if date_time:
            # Extract month information (simple pattern matching)
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            for month in months:
                if month in date_time:
                    stats['date_range']['events_by_month'][month] = stats['date_range']['events_by_month'].get(month, 0) + 1
                    break
    
    return stats

def save_parsed_data(events: List[Dict[str, Any]], output_file: str, fetch_metadata: Dict[str, Any] = None):
    """Save all parsed data to JSON file"""
    try:
        # Create comprehensive output structure
        output_data = {
            'metadata': {
                'parsing_timestamp': time.time(),
                'parsing_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'source': 'yes24_fetch_and_parse',
                'fetch_metadata': fetch_metadata or {}
            },
            'summary_statistics': create_summary_stats(events),
            'events': events
        }
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✔ Data successfully saved to: {output_file}")
        print(f"  Total events processed: {len(events)}")
        print(f"  Genres found: {len(output_data['summary_statistics']['genres'])}")
        print(f"  Venues found: {len(output_data['summary_statistics']['venues'])}")
        print(f"  Parsing errors: {output_data['summary_statistics']['errors']}")
        print(f"  File size: {Path(output_file).stat().st_size:,} bytes")
        
    except Exception as e:
        print(f"✗ Error saving data: {e}")

# ------------------------------------------------------------------ #
# 3) Main combined function
# ------------------------------------------------------------------ #
def fetch_and_parse_yes24_data(genre: str = 'concert', output_file: str = None):
    """
    Main function to fetch and parse Yes24 data in one step
    
    Args:
        genre: Genre to fetch ('all', 'concert', 'musical', 'play', 'exhibition', 'classical')
        output_file: Output JSON file path (auto-generated if None)
    """
    
    print("=" * 60)
    print("YES24 TICKET DATA FETCHER & PARSER")
    print("=" * 60)
    
    # Step 1: Fetch data from Yes24
    print(f"\n📡 Fetching data for genre: {genre}")
    html_content = fetch_yes24_data(genre)
    if not html_content:
        print("✗ Failed to fetch data. Exiting.")
        return False
    
    # Step 2: Create fetch metadata
    fetch_metadata = {
        'genre': genre,
        'genre_id': GENRES.get(genre, ''),
        'html_size': len(html_content),
        'contains_yes24': 'yes24' in html_content.lower(),
        'contains_list_wrap': 'list_wrap' in html_content
    }
    
    print(f"\n🔍 Processing HTML content...")
    print("-" * 40)
    
    # Step 3: Parse the HTML and extract events
    events = parse_yes24_html(html_content)
    
    if not events:
        print("✗ No events found in the fetched HTML")
        return False
    
    # Step 4: Generate output filename if not provided
    if not output_file:
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        output_file = f"yes24_{genre}_events_{timestamp}.json"
    
    # Step 5: Save all data
    save_parsed_data(events, output_file, fetch_metadata)
    
    # Step 6: Print sample of extracted data
    if events:
        print(f"\n📋 Sample of extracted fields from first event:")
        sample_event = events[0]
        if 'parsing_error' not in sample_event:
            print(f"  ID: {sample_event.get('event_id', 'N/A')}")
            print(f"  Title: {sample_event.get('title', 'N/A')}")
            details = sample_event.get('details', {})
            print(f"  Genre: {details.get('genre', 'N/A')}")
            print(f"  Venue: {details.get('venue', 'N/A')}")
            print(f"  Date/Time: {details.get('date_time', 'N/A')}")
            print(f"  Duration: {details.get('duration', 'N/A')}")
        else:
            print(f"  First event had parsing error: {sample_event.get('parsing_error')}")
    
    return True

# ------------------------------------------------------------------ #
# 4) Entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch and parse Yes24 ticket data into structured JSON"
    )
    parser.add_argument(
        '--genre', '-g', 
        type=str, 
        default='concert', 
        choices=['all', 'concert', 'musical', 'play', 'exhibition', 'classical'],
        help='Genre to fetch (default: concert)'
    )
    parser.add_argument(
        '--output', '-o', 
        type=str, 
        default=None, 
        help='Output JSON file path (auto-generated if not specified)'
    )
    
    args = parser.parse_args()
    
    # Run the combined fetch and parse operation
    success = fetch_and_parse_yes24_data(
        genre=args.genre,
        output_file=args.output
    )
    
    if success:
        print("\n🎉 Fetch and parse operation completed successfully!")
    else:
        print("\n❌ Fetch and parse operation failed!")
        sys.exit(1)
