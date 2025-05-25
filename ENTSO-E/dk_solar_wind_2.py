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

# </editor-fold>





EEP = EntsoeDataProcessor(os.getenv("API_KEY"))

# Variables
# Dates are in YYYY-MM-DD format
start_date1 = "2020-01-01"
end_date1 = "2025-05-24"

start_date2 = "2024-12-07"
end_date2 = "2025-05-24"

domain_eic = "10Y1001A1001A796"
psr_name_1 = "Anholt"
psr_name_3 = "DK_KF_AB_GU"
psr_name_2 = "Horns Rev C"
psr_name_4 = "Solar Park Gedmosen"
psr_name_5 = "Solar Park Holsted"



# --- Configuration for PSR 4 ---

gedmosen_csv_filename = f"{psr_name_4.replace(' ', '_')}_{start_date1}_to_{end_date1}.npy"

gedmosen_np_array = load_numpy_from_npy(gedmosen_csv_filename)
if gedmosen_np_array is None:
    print(f"No local data found for {psr_name_4}. Fetching from API...")
    gedmosen_np_array = EEP.fetch_and_process_psr_data_range(
        overall_start_date_str=start_date1,
        overall_end_date_str=end_date1,
        domain_eic=domain_eic,
        psr_name_to_extract=psr_name_4
        # time_hour_minute parameter defaults to "2200" in the class method
    )
    if gedmosen_np_array is not None:

        actual_dates_gedmosen= get_actual_date_strings_for_filename(gedmosen_np_array)
        if actual_dates_gedmosen:
            actual_start_str_gedmosen, actual_end_str_gedmosen = actual_dates_gedmosen
            filename_to_save = f"{psr_name_4.replace(' ', '_')}_{actual_start_str_gedmosen}_to_{actual_end_str_gedmosen}.npy"
            print(f"Actual dates for {psr_name_1}: {start_date1} to {end_date1}")
        else:
            print(f"Could not extract actual dates for {psr_name_1}. Using original dates.")

        save_numpy_to_npy(filename_to_save, gedmosen_np_array)
    else:
        print(f"Failed to fetch data for {psr_name_4}.")

if gedmosen_np_array is not None:
    print(f"\nData for {psr_name_4}:")
    print(f"Shape: {gedmosen_np_array.shape}")
    # print(gedmosen_np_array[:5]) # Print first 5 rows
else:
    print(f"Could not obtain data for {psr_name_4}.")


print("\nSleeping for a few seconds before next PSR (if fetching)...")
# You might only need a long sleep if both fetches are new.
# If data is loaded from CSV, no API call is made.
sleep(5) # Reduced sleep for testing when data might be local

# --- Configuration for PSR 5 ---

holsted_csv_filename = f"{psr_name_5.replace(' ', '_')}_{start_date1}_to_{end_date1}.npy"

holsted_np_array = load_numpy_from_npy(holsted_csv_filename)
if holsted_np_array is None:
    print(f"No local data found for {psr_name_5}. Fetching from API...")
    holsted_np_array = EEP.fetch_and_process_psr_data_range(
        overall_start_date_str=start_date1,
        overall_end_date_str=end_date1,
        domain_eic=domain_eic,
        psr_name_to_extract=psr_name_5
    )
    if holsted_np_array is not None:

        actual_dates_holsted= get_actual_date_strings_for_filename(holsted_np_array)
        if actual_dates_holsted:
            actual_start_str_holsted, actual_end_str_holsted = actual_dates_holsted
            filename_to_save = f"{psr_name_5.replace(' ', '_')}_{actual_start_str_holsted}_to_{actual_end_str_holsted}.npy"
            print(f"Actual dates for {psr_name_1}: {start_date1} to {end_date1}")
        else:
            print(f"Could not extract actual dates for {psr_name_1}. Using original dates.")

        save_numpy_to_npy(filename_to_save, holsted_np_array)
    else:
        print(f"Failed to fetch data for {psr_name_5}.")

if holsted_np_array is not None:
    print(f"\nData for {psr_name_5}:")
    print(f"Shape: {holsted_np_array.shape}")
    # print(holsted_np_array[:5]) # Print first 5 rows
else:
    print(f"Could not obtain data for {psr_name_5}.")







