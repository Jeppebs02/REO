import os
from time import sleep
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

# </editor-fold>





EEP = EntsoeDataProcessor(os.getenv("API_KEY"))

# Variables
# Dates are in YYYY-MM-DD format
start_date = "2024-01-01"
end_date = "2025-05-24"
domain_eic = "10Y1001A1001A796"
psr_name_1 = "Solar Park Gedmosen"
psr_name_2 = "Solar Park Holsted"

# --- Configuration for PSR 1 ---

gedmosen_csv_filename = f"{psr_name_1.replace(' ', '_')}_{start_date}_to_{end_date}.csv"

gedmosen_np_array = load_numpy_from_npy(gedmosen_csv_filename)
if gedmosen_np_array is None:
    print(f"No local data found for {psr_name_1}. Fetching from API...")
    gedmosen_np_array = EEP.fetch_and_process_psr_data_range(
        overall_start_date_str=start_date,
        overall_end_date_str=end_date,
        domain_eic=domain_eic,
        psr_name_to_extract=psr_name_1
        # time_hour_minute parameter defaults to "2200" in the class method
    )
    if gedmosen_np_array is not None:
        save_numpy_to_npy(gedmosen_csv_filename, gedmosen_np_array)
    else:
        print(f"Failed to fetch data for {psr_name_1}.")

if gedmosen_np_array is not None:
    print(f"\nData for {psr_name_1}:")
    print(f"Shape: {gedmosen_np_array.shape}")
    # print(gedmosen_np_array[:5]) # Print first 5 rows
else:
    print(f"Could not obtain data for {psr_name_1}.")


print("\nSleeping for a few seconds before next PSR (if fetching)...")
# You might only need a long sleep if both fetches are new.
# If data is loaded from CSV, no API call is made.
sleep(5) # Reduced sleep for testing when data might be local

# --- Configuration for PSR 2 ---
psr_name_2 = "Solar Park Holsted"
holsted_csv_filename = f"{psr_name_2.replace(' ', '_')}_{start_date}_to_{end_date}.csv"

holsted_np_array = load_numpy_from_npy(holsted_csv_filename)
if holsted_np_array is None:
    print(f"No local data found for {psr_name_2}. Fetching from API...")
    holsted_np_array = EEP.fetch_and_process_psr_data_range(
        overall_start_date_str=start_date,
        overall_end_date_str=end_date,
        domain_eic=domain_eic,
        psr_name_to_extract=psr_name_2
    )
    if holsted_np_array is not None:
        save_numpy_to_npy(holsted_csv_filename, holsted_np_array)
    else:
        print(f"Failed to fetch data for {psr_name_2}.")

if holsted_np_array is not None:
    print(f"\nData for {psr_name_2}:")
    print(f"Shape: {holsted_np_array.shape}")
    # print(holsted_np_array[:5]) # Print first 5 rows
else:
    print(f"Could not obtain data for {psr_name_2}.")


if gedmosen_np_array is not None and holsted_np_array is not None:
    print("\nArrays for all requested PSRs obtained (either fetched or loaded).")
else:
    print("\nCould not obtain data for one or more PSRs.")



