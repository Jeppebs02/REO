"""
Fetches 15-minute production data for four Danish solar farms:
  - Solar Park Holsted
  - Solar Park Kassoe
  - Solar Park Gedmosen
  - Solar Park Viuf og Håstrup  (TODO: verify exact name via discover_psr_names.py)

Period: 2025-01-01 → 2026-06-12
Output: CSV files in the current working directory (run from ENTSO-E/)

Resolution note: ENTSO-E switched Danish solar farms from PT60M to PT15M mid-2025.
EntsoEClient handles both transparently — no special-casing needed here.

Requires the API_KEY environment variable to be set.
"""
import os
from time import sleep
from typing import Sequence

import numpy as np

from entsoe import EntsoEClient


# ---------------------------------------------------------------------------
# Helpers (same as dk_solar_wind_2.py)
# ---------------------------------------------------------------------------

def save_numpy_to_csv(filepath: str, array_data: np.ndarray):
    print(f"Saving data to {filepath}...")
    np.savetxt(filepath, array_data, delimiter=",", fmt="%s",
               header="Timestamp,Quantity_MW", comments="")
    print(f"Data saved successfully to {filepath}.")


def load_numpy_from_csv(filepath: str) -> np.ndarray | None:
    if os.path.exists(filepath):
        print(f"Loading data from {filepath}...")
        try:
            array_data = np.loadtxt(filepath, delimiter=",", dtype=object, skiprows=1)
            print(f"Data loaded successfully from {filepath}.")
            return array_data
        except Exception as e:
            print(f"Error loading data from {filepath}: {e}")
    return None


def get_actual_date_strings_for_filename(array_data: np.ndarray) -> tuple[str, str] | None:
    if array_data is None or array_data.ndim != 2 or array_data.shape[0] == 0 or array_data.shape[1] < 1:
        return None
    try:
        start_yyyymmdd = str(array_data[0, 0])[:8]
        end_yyyymmdd   = str(array_data[-1, 0])[:8]
        actual_start = f"{start_yyyymmdd[:4]}-{start_yyyymmdd[4:6]}-{start_yyyymmdd[6:8]}"
        actual_end   = f"{end_yyyymmdd[:4]}-{end_yyyymmdd[4:6]}-{end_yyyymmdd[6:8]}"
        return actual_start, actual_end
    except Exception as e:
        print(f"Error extracting date strings from array: {e}")
        return None


def process_psrs(
        psr_names: Sequence[str],
        start_date: str,
        end_date: str,
        domain_eic: str,
        eep: EntsoEClient,
        time_hour_minute: str = "0000",
        sleep_seconds: int = 5,
        pad_missing_days: bool = False,
        fill_value=np.nan,
        save_as_csv: bool = True,
        registered_resources: dict[str, str] | None = None,
) -> None:
    for psr in psr_names:
        print(f"\n--- Processing PSR: {psr} ---")
        base = psr.replace(" ", "_")
        extension = ".csv" if save_as_csv else ".npy"
        default_file = f"{base}_{start_date}_to_{end_date}{extension}"

        data = load_numpy_from_csv(default_file) if save_as_csv else np.load(default_file) if os.path.exists(default_file) else None

        if data is None:
            resource_code = (registered_resources or {}).get(psr)
            if resource_code:
                print(f"  Using RegisteredResource code: {resource_code}")
            print(f"No cached file found ({default_file}). Fetching from API...")
            data = eep.fetch_and_process_psr_data_range_new(
                overall_start_date_str=start_date,
                overall_end_date_str=end_date,
                domain_eic=domain_eic,
                psr_name_to_extract=psr,
                time_hour_minute=time_hour_minute,
                pad_missing_days=pad_missing_days,
                fill_value=fill_value,
                registered_resource=resource_code,
            )
            if data is None or data.size == 0:
                print(f"✗ Failed to fetch data for {psr}")
                continue

            actual = get_actual_date_strings_for_filename(data)
            if actual:
                a_start, a_end = actual
                filename_to_save = f"{base}_{a_start}_to_{a_end}{extension}"
                print(f"✓ Actual date range for {psr}: {a_start} → {a_end}")
            else:
                filename_to_save = default_file
                print(f"Could not extract actual dates; using requested range for filename.")

            if save_as_csv:
                save_numpy_to_csv(filename_to_save, data)
            else:
                np.save(filename_to_save, data)
        else:
            print(f"✓ Loaded cached data for {psr} from {default_file}")

        print(f"Data shape for {psr}: {data.shape}")
        sleep(sleep_seconds)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VIUF_HASTRUP_PSR_NAME = "Solar Park Viuf and Håstrup"

PSR_LIST = [
    "Solar Park Holsted",
    "Solar Park Kassoe",
    "Solar Park Gedmosen",
    VIUF_HASTRUP_PSR_NAME,
]

# Unit codes from docs/ENTSO-E/Other/BZN DK1 Generation Units.md
# Used as RegisteredResource= API param to avoid name-matching on non-ASCII characters
REGISTERED_RESOURCES = {
    "Solar Park Holsted":          "45W000000000214N",
    "Solar Park Kassoe":           "45W000000000209G",
    "Solar Park Gedmosen":         "45W000000000210V",
    VIUF_HASTRUP_PSR_NAME:         "45W000000000221Q",
}

START_DATE = "2025-01-01"
END_DATE   = "2026-06-12"
DOMAIN_DK1 = "10Y1001A1001A796"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    eep = EntsoEClient(os.getenv("API_KEY"))

    process_psrs(
        psr_names=PSR_LIST,
        start_date=START_DATE,
        end_date=END_DATE,
        domain_eic=DOMAIN_DK1,
        eep=eep,
        time_hour_minute="0000",
        sleep_seconds=5,
        pad_missing_days=True,
        fill_value=0,
        save_as_csv=True,
        registered_resources=REGISTERED_RESOURCES,
    )
