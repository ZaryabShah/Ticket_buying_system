"""
prodlist_parser.py

Self‑contained parser for Ticketmelon‑style `prodList_*.json` dumps that writes **one
clean structured JSON file** — no command‑line flags needed.

How it works
============
1. Put this script next to any *input* file(s) named like `prodList_*.json`.
2. Run `python prodlist_parser.py`.
3. It finds the newest matching file, parses **every field** (including nested
   `seatGradeJson`, `saleTypeJson`, `perfRelatJson` …), attaches flattened
   lists back onto each event, and writes `<input>_parsed.json`.

The output structure is:
```
{
  "source_file": "prodList_HIT.json",
  "extracted_at": "2025-07-04T15:32:10.213Z",
  "events": [
     {
       … all original top‑level keys …,
       "seatGrades": [ { seatGradeNo, seatGradeName, basePrice, … }, … ],
       "saleTypes":  [ { pocName, pocCode, saleTypeCode, saleTypeName, … }, … ],
       "perfRelat":  [ … ]
     }, …
  ]
}
```
Just open the resulting JSON and everything is there in one place — ready for
ETL, analytics, or forwarding to another API.
"""

from __future__ import annotations

import glob
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NESTED_JSON_FIELDS = {
    "seatGradeJson",
    "saleTypeJson",
    "perfRelatJson",
}


def _safe_json_load(raw: str | dict | list | None) -> Any:
    """Return parsed JSON if *raw* is a JSON string, else return *raw* unchanged."""
    if not isinstance(raw, str):
        return raw
    raw = raw.strip()
    if not raw:
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw  # Leave as‑is if it is not valid JSON


# ---------------------------------------------------------------------------
# Core parsing routines
# ---------------------------------------------------------------------------

def _decode_nested(event: Dict[str, Any]) -> Dict[str, Any]:
    """Decode known nested JSON columns in‑place and return *event*."""
    for field in NESTED_JSON_FIELDS:
        if field in event:
            event[field] = _safe_json_load(event[field])
    return event


def _attach_lists(event: Dict[str, Any]) -> None:
    """Add *seatGrades* and *saleTypes* lists derived from nested objects."""
    # ---------------- Seat grades ----------------
    seat_grades: List[Dict[str, Any]] = []
    sg_root = event.get("seatGradeJson")
    if isinstance(sg_root, dict):
        for item in sg_root.get("data", {}).get("list", []):
            seat_grades.append(item)
    event["seatGrades"] = seat_grades

    # ---------------- Sale types ----------------
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

    # ---------------- perfRelat (keep raw list) ----------------
    pr_root = event.get("perfRelatJson")
    if isinstance(pr_root, dict):
        event["perfRelat"] = pr_root.get("data", {}).get("list", [])
    else:
        event["perfRelat"] = []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_prodlist(path: str | Path) -> Dict[str, Any]:
    """Return structured JSON‑serialisable data from a Ticketmelon prodList dump."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        doc = json.load(f)

    raw_events: List[Dict[str, Any]] = doc.get("data", [])
    events: List[Dict[str, Any]] = []

    for ev in raw_events:
        ev = _decode_nested(ev)
        _attach_lists(ev)
        events.append(ev)

    return {
        "source_file": path.name,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "events": events,
    }


# ---------------------------------------------------------------------------
# CLI‑less runner
# ---------------------------------------------------------------------------

def _find_latest_dump() -> Path | None:
    """Return newest file matching prodList_*.json or None."""
    candidates = sorted(glob.glob("prodList_*.json"))
    return Path(candidates[-1]) if candidates else None


def main() -> None:
    infile = _find_latest_dump()
    if infile is None:
        print("No prodList_*.json file found in this directory.", file=sys.stderr)
        sys.exit(1)

    print(f"Parsing {infile} …")
    result = parse_prodlist(infile)

    outfile = infile.with_stem(infile.stem + "_parsed").with_suffix(".json")
    with outfile.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✔ Wrote structured data to {outfile}")


if __name__ == "__main__":
    main()
