#!/usr/bin/env python3
"""
simple_parser.py
Author: Assistant, 2025-07-04

Simple parser that extracts all data from the endProdList txt file
and saves it to a structured JSON file.

No external requests - just parses the existing data.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

def parse_txt_file(file_path: str) -> Optional[Dict[str, Any]]:
    """Parse the endProdList txt file and return JSON data"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # Parse JSON
        data = json.loads(content)
        print(f"✔ Successfully parsed {file_path}")
        
        events = data.get('endProdList', [])
        print(f"  Found {len(events)} events")
        print(f"  Total count: {data.get('listCount', 'Unknown')}")
        
        return data
        
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in {file_path}: {e}")
        return None
    except Exception as e:
        print(f"✗ Error parsing {file_path}: {e}")
        return None

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

def parse_events_from_txt(txt_file: str, output_file: str = None):
    """
    Main function to parse txt file and extract all event data
    
    Args:
        txt_file: Path to the endProdList txt file
        output_file: Output JSON file path (auto-generated if None)
    """
    
    print("=" * 60)
    print("MELON TICKET DATA PARSER")
    print("=" * 60)
    
    # Parse the txt file
    print(f"Parsing: {txt_file}")
    parsed_data = parse_txt_file(txt_file)
    if not parsed_data:
        return
    
    # Extract events
    events_raw = parsed_data.get('endProdList', [])
    if not events_raw:
        print("✗ No events found in the data")
        return
    
    print(f"\nExtracting data from {len(events_raw)} events...")
    print("-" * 40)
    
    # Process each event
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
    
    # Generate output filename if not provided
    if not output_file:
        txt_path = Path(txt_file)
        output_file = txt_path.parent / f"{txt_path.stem}_parsed.json"
    
    # Save all data
    save_parsed_data(parsed_data, processed_events, output_file)
    
    # Print sample of extracted data
    if processed_events:
        print(f"\n📋 Sample of extracted fields from first event:")
        sample_event = processed_events[0]
        print(f"  ID: {sample_event.get('id')}")
        print(f"  Title (EN): {sample_event.get('titles', {}).get('english')}")
        print(f"  Performance Type: {sample_event.get('performance_type', {}).get('english')}")
        print(f"  Venue: {sample_event.get('venue', {}).get('name_en')}")
        print(f"  Period: {sample_event.get('schedule', {}).get('period_info')}")

# ------------------------------------------------------------------ #
# CLI Interface
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    import sys
    
    # Default input file
    default_input = "raw.txt"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = default_input
    
    # Check if file exists
    if not Path(input_file).exists():
        print(f"✗ File not found: {input_file}")
        print(f"Usage: python simple_parser.py [input_file.txt] [output_file.json]")
        print(f"Example: python simple_parser.py raw.txt events_parsed.json")
        sys.exit(1)
    
    # Get output file if specified
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the parser
    parse_events_from_txt(
        txt_file=input_file,
        output_file=output_file
    )
