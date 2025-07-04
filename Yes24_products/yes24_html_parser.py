#!/usr/bin/env python3
"""
yes24_html_parser.py
Author: Assistant, 2025-07-04

Parser that extracts all event/concert data from Yes24 HTML pages
and saves it to a structured JSON file.

Supports both file parsing and direct HTML string parsing.
"""

import json
import time
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup

def parse_html_file(file_path: str) -> Optional[str]:
    """Parse the HTML file and return HTML content"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"✔ Successfully loaded {file_path}")
        print(f"  File size: {len(content):,} characters")
        
        return content
        
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        return None
    except Exception as e:
        print(f"✗ Error reading {file_path}: {e}")
        return None

def extract_event_id(url: str) -> Optional[str]:
    """Extract event ID from Yes24 URL"""
    try:
        # Extract IdPerf parameter from URL
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
            'source': 'yes24_html_parser'
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

def save_parsed_data(events: List[Dict[str, Any]], output_file: str, html_metadata: Dict[str, Any] = None):
    """Save all parsed data to JSON file"""
    try:
        # Create comprehensive output structure
        output_data = {
            'metadata': {
                'parsing_timestamp': time.time(),
                'parsing_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'source': 'yes24_html_parser',
                'html_metadata': html_metadata or {}
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

def parse_yes24_html_file(html_file: str, output_file: str = None):
    """
    Main function to parse Yes24 HTML file and extract all event data
    
    Args:
        html_file: Path to the HTML file
        output_file: Output JSON file path (auto-generated if None)
    """
    
    print("=" * 60)
    print("YES24 HTML PARSER")
    print("=" * 60)
    
    # Parse the HTML file
    print(f"Loading HTML file: {html_file}")
    html_content = parse_html_file(html_file)
    if not html_content:
        return False
    
    # Extract HTML metadata
    html_metadata = {
        'file_path': html_file,
        'file_size': len(html_content),
        'contains_yes24': 'yes24' in html_content.lower(),
        'contains_concert': 'concert' in html_content.lower()
    }
    
    print(f"\nExtracting events from HTML...")
    print("-" * 40)
    
    # Parse the HTML and extract events
    events = parse_yes24_html(html_content)
    
    if not events:
        print("✗ No events found in the HTML")
        return False
    
    # Generate output filename if not provided
    if not output_file:
        html_path = Path(html_file)
        output_file = html_path.parent / f"{html_path.stem}_parsed.json"
    
    # Save all data
    save_parsed_data(events, output_file, html_metadata)
    
    # Print sample of extracted data
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
            print(f"  Age Group: {details.get('age_group', 'N/A')}")
        else:
            print(f"  First event had parsing error: {sample_event.get('parsing_error')}")
    
    return True

# ------------------------------------------------------------------ #
# CLI Interface
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    import sys
    
    # Default input file
    default_input = "response.html"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = default_input
    
    # Check if file exists
    if not Path(input_file).exists():
        print(f"✗ File not found: {input_file}")
        print(f"Usage: python yes24_html_parser.py [input_file.html] [output_file.json]")
        print(f"Example: python yes24_html_parser.py response.html yes24_events.json")
        sys.exit(1)
    
    # Get output file if specified
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the parser
    success = parse_yes24_html_file(
        html_file=input_file,
        output_file=output_file
    )
    
    if success:
        print("\n🎉 HTML parsing completed successfully!")
    else:
        print("\n❌ HTML parsing failed!")
        sys.exit(1)
