import os
from time import sleep
from typing import Sequence
import numpy as np

from EntsoEDataProcessor import EntsoeDataProcessor

# <editor-fold desc="Miscellaneous Functions">

def save_numpy_to_npy(filepath: str, array_data: np.ndarray):
    """Saves a NumPy array to a .npy file."""
    print(f"Saving data to {filepath}...")
    np.save(filepath, array_data)
    print(f"Data saved successfully to {filepath}.")

def load_numpy_from_npy(filepath: str) -> np.ndarray | None:
    """Loads a NumPy array from a .npy file."""
    if os.path.exists(filepath):
        print(f"Loading data from {filepath}...")
        try:
            array_data = np.load(filepath)
            print(f"Data loaded successfully from {filepath}.")
            return array_data
        except Exception as e:
            print(f"Error loading data from {filepath}: {e}")
            return None
    return None


def get_actual_date_strings_for_filename(array_data: np.ndarray) -> tuple[str, str] | None:
    if array_data is None or array_data.ndim != 2 or array_data.shape[0] == 0 or array_data.shape[1] < 1:
        return None
    try:
        # First column, first row, first 8 chars (YYYYMMDD)
        start_yyyymmdd = str(array_data[0, 0])[:8]
        # First column, last row, first 8 chars (YYYYMMDD)
        end_yyyymmdd = str(array_data[-1, 0])[:8]

        # Format to YYYY-MM-DD
        actual_start_date_str = f"{start_yyyymmdd[:4]}-{start_yyyymmdd[4:6]}-{start_yyyymmdd[6:8]}"
        actual_end_date_str = f"{end_yyyymmdd[:4]}-{end_yyyymmdd[4:6]}-{end_yyyymmdd[6:8]}"
        return actual_start_date_str, actual_end_date_str
    except Exception as e:
        print(f"Error extracting date strings from array: {e}")
        return None



def process_psrs(
    psr_names: Sequence[str],
    start_date: str,
    end_date: str,
    domain_eic: str,
    eep,                       # EntsoeDataProcessor instance
    time_hour_minute: str = "2200",
    sleep_seconds: int = 5,
) -> None:
    """
    Load or fetch data for several PSRs and persist each as an .npy file.

    Parameters
    ----------
    psr_names : sequence[str]
        PSR names to process.
    start_date, end_date : str
        Date range in YYYY-MM-DD format.
    domain_eic : str
        EIC of the domain.
    eep : EntsoeDataProcessor
        Pre-initialised data processor.
    time_hour_minute : str, default '2200'
        Settlement period requested by Entsoe API.
    sleep_seconds : int, default 5
        Pause between API calls.
    """
    for psr in psr_names:
        base = psr.replace(" ", "_")
        default_file = f"{base}_{start_date}_to_{end_date}.npy"

        data = load_numpy_from_npy(default_file)
        if data is None:
            print(f"No local data for {psr}. Fetching...")
            data = eep.fetch_and_process_psr_data_range(
                overall_start_date_str=start_date,
                overall_end_date_str=end_date,
                domain_eic=domain_eic,
                psr_name_to_extract=psr,
                time_hour_minute=time_hour_minute,
            )
            if data is None:
                print(f"✗ Failed to fetch data for {psr}")
                continue

            # Try to reflect the true span in the filename
            actual = get_actual_date_strings_for_filename(data)
            if actual:
                a_start, a_end = actual
                filename_to_save = f"{base}_{a_start}_to_{a_end}.npy"
                print(f"✓ Actual dates for {psr}: {a_start} → {a_end}")
            else:
                filename_to_save = default_file
                print(f"Couldn’t extract dates for {psr}; using requested range.")

            save_numpy_to_npy(filename_to_save, data)
        else:
            print(f"✓ Loaded cached data for {psr}")

        # Diagnostics
        print(f"Data shape for {psr}: {data.shape if data is not None else 'N/A'}")
        print("-" * 40)
        sleep(sleep_seconds)

# </editor-fold>





EEP = EntsoeDataProcessor(os.getenv("API_KEY"))

# Variables
# Dates are in YYYY-MM-DD format
start_date1 = "2025-01-01"
end_date1 = "2025-12-31"

start_date2 = "2025-01-01"
end_date2 = "2025-12-31"

domain_eic = "10Y1001A1001A796"
psr_name_1 = "Vesterhav Syd"
psr_name_3 = "Vesterhav Nord"
psr_name_2 = "Horns Rev C"
psr_name_4 = "Solar Park Gedmosen"
psr_name_5 = "Solar Park Holsted"
psr_name_6 = "Solar Park Kassoe"
psr_name_7 = "Horns rev A"
psr_name_8 = "Horns rev B"
psr_name_9 = "Horns rev C"

PSR_LIST = [
    "Solar Park Kassoe",
    "Solar Park Gedmosen",
    "Solar Park Holsted",
    "Vesterhav Syd",
    "Vesterhav Nord",
    "Horns Rev A",
    "Horns Rev B",
    "Horns Rev C",
    # add more as needed
]

PSR_LIST2 = [

    "Horns Rev A",
    "Horns Rev B",
    "Horns Rev C",
    # add more as needed
]


# Process each PSR
process_psrs(
    psr_names=PSR_LIST,
    start_date=start_date1,
    end_date=end_date1,
    domain_eic=domain_eic,
    eep=EEP,
    time_hour_minute="2200",
    sleep_seconds=5,
)






