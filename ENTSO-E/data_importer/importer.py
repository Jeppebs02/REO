"""
ENTSO-E Data Importer
=====================
Fetches 15-minute electricity generation data for Danish generation units
and saves each unit as a CSV file in the output/ folder.

Data source: ENTSO-E Transparency Platform — Actual Generation per Generation Unit (A73)
Output format: CSV with columns  Timestamp (YYYYMMDDH),  Quantity_MW
               96 rows per calendar day (one row per 15-minute interval)

HOW TO RUN
----------
    python importer.py

(Run from any directory — the script finds its own paths automatically.)
"""

import os
import sys
from time import sleep
from typing import Sequence

import numpy as np

# ── Allow importing EntsoEDataProcessor from the parent ENTSO-E/ directory ───
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from EntsoEDataProcessor import EntsoeDataProcessor


# =============================================================================
#  CONFIGURATION  ←  Only edit this section
# =============================================================================

# ── STEP 1: Your ENTSO-E API key ─────────────────────────────────────────────
#   Get one at: https://transparency.entsoe.eu  →  My Account Settings → Web API
API_KEY = "PASTE_YOUR_API_KEY_HERE"


# ── STEP 2: Date range ───────────────────────────────────────────────────────
#   Format: YYYY-MM-DD
START_DATE = "2025-01-01"
END_DATE   = "2026-06-12"


# ── STEP 3: Bidding zone ─────────────────────────────────────────────────────
#   "DK1"  →  Western Denmark (Jutland + Funen)
#   "DK2"  →  Eastern Denmark (Zealand + Bornholm)
ZONE = "DK1"


# ── STEP 4: Generation units to import ───────────────────────────────────────
#   Remove or comment out (#) any unit you do NOT want.
#
#   DK1 — Western Denmark units:
PSR_LIST_DK1 = [
    "Skærbækværket 3",           # Fossil Gas
    "Studstrupværket 3",         # Fossil Hard Coal
    "Solar Park Viuf and Håstrup",  # Solar
    "Solar Park Holsted",        # Solar
    "Solar Park Kassoe",         # Solar
    "Solar Park Gedmosen",       # Solar
    "Vesterhav Nord",            # Wind Offshore
    "Vesterhav Syd",             # Wind Offshore
    "Horns Rev C",               # Wind Offshore
]

#   DK2 — Eastern Denmark units:
PSR_LIST_DK2 = [
    "Avedøreværket 1",           # Fossil Gas
    "Avedøreværket 2",           # Fossil Gas
    "Amagerværket 4",            # Biomass
    "Solar Park Vedde",          # Solar
    "Solar Park Lidsø",          # Solar
    "Rødsand 1",                 # Wind Offshore
    "DK_KF_AB_GU",               # Wind Offshore (Kriegers Flak)
]

# =============================================================================
#  END OF CONFIGURATION  ←  Do not edit below this line
# =============================================================================


# ── Zone EIC codes ────────────────────────────────────────────────────────────
_ZONE_EIC = {
    "DK1": "10Y1001A1001A796",
    "DK2": "10YDK-2--------M",
}

# ── Output folder (data_importer/output/) ────────────────────────────────────
_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")


# ── Helper functions ──────────────────────────────────────────────────────────

def _save_csv(filepath: str, array: np.ndarray) -> None:
    print(f"  Saving → {filepath}")
    np.savetxt(filepath, array, delimiter=",", fmt="%s",
               header="Timestamp,Quantity_MW", comments="")


def _load_csv(filepath: str) -> np.ndarray | None:
    if os.path.exists(filepath):
        try:
            return np.loadtxt(filepath, delimiter=",", dtype=object, skiprows=1)
        except Exception as e:
            print(f"  Warning: could not read cached file ({e}). Re-fetching.")
    return None


def _actual_dates(array: np.ndarray) -> tuple[str, str] | None:
    """Extract first and last YYYY-MM-DD from the timestamp column."""
    try:
        s = str(array[0, 0])[:8]
        e = str(array[-1, 0])[:8]
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}", f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    except Exception:
        return None


def _fetch_units(
        psr_names: Sequence[str],
        start_date: str,
        end_date: str,
        domain_eic: str,
        processor: EntsoeDataProcessor,
) -> None:
    os.makedirs(_OUTPUT_DIR, exist_ok=True)

    for psr in psr_names:
        print(f"\n{'─'*60}")
        print(f"  Unit: {psr}")

        safe_name   = psr.replace(" ", "_")
        cache_file  = os.path.join(_OUTPUT_DIR, f"{safe_name}_{start_date}_to_{end_date}.csv")
        cached_data = _load_csv(cache_file)

        if cached_data is not None:
            print(f"  ✓ Already downloaded — loaded from cache ({cached_data.shape[0]} rows)")
            continue

        print(f"  Fetching {start_date} → {end_date} from ENTSO-E ...")
        data = processor.fetch_and_process_psr_data_range_new(
            overall_start_date_str=start_date,
            overall_end_date_str=end_date,
            domain_eic=domain_eic,
            psr_name_to_extract=psr,
            time_hour_minute="0000",
            pad_missing_days=True,
            fill_value=0,
        )

        if data is None or data.size == 0:
            print(f"  ✗ No data returned for {psr}. Check the unit name and date range.")
            continue

        dates = _actual_dates(data)
        if dates:
            actual_start, actual_end = dates
            save_path = os.path.join(_OUTPUT_DIR, f"{safe_name}_{actual_start}_to_{actual_end}.csv")
        else:
            save_path = cache_file

        _save_csv(save_path, data)
        print(f"  ✓ Done — {data.shape[0]} rows ({data.shape[0] // 96} days × 96 intervals)")
        sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    # Guard: catch unfilled API key
    if API_KEY == "PASTE_YOUR_API_KEY_HERE" or not API_KEY.strip():
        print("=" * 60)
        print("  ERROR: No API key configured.")
        print("  Open importer.py and paste your ENTSO-E API key")
        print("  into the API_KEY variable at the top of the file.")
        print("=" * 60)
        return

    # Guard: validate zone
    if ZONE not in _ZONE_EIC:
        print(f"ERROR: ZONE must be 'DK1' or 'DK2', got '{ZONE}'.")
        return

    domain_eic = _ZONE_EIC[ZONE]
    psr_list   = PSR_LIST_DK1 if ZONE == "DK1" else PSR_LIST_DK2

    if not psr_list:
        print(f"ERROR: PSR_LIST_{ZONE} is empty — add at least one unit name.")
        return

    print("=" * 60)
    print(f"  ENTSO-E Data Importer")
    print(f"  Zone       : {ZONE}")
    print(f"  Period     : {START_DATE}  →  {END_DATE}")
    print(f"  Units      : {len(psr_list)}")
    print(f"  Output dir : {_OUTPUT_DIR}")
    print("=" * 60)

    processor = EntsoeDataProcessor(API_KEY)
    _fetch_units(psr_list, START_DATE, END_DATE, domain_eic, processor)

    print(f"\n{'='*60}")
    print(f"  Import complete.  Files are in:  {_OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
