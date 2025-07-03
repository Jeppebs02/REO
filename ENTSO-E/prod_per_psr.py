# In your main script (e.g., DKSOLAR.py)

import os
import numpy as np
from EntsoEDataProcessor import EntsoeDataProcessor  # Make sure to import your updated class

# Define the production types and their column order
# This is crucial for consistency
PRODUCTION_TYPES_COLUMNS = [
    'B05',  # Column 0: Fossil Hard coal
    'B01'  # Column 1: Biomass
]

# Define the bidding zone EICs
DK1_EIC = "10YDK-1--------W"
DK2_EIC = "10YDK-2--------M"

# Create the explanation file
explanation_text = f"""
Data columns for DK1 and DK2 production arrays:
- File format: NumPy array (.npy)
- Time resolution: 15-minute intervals.
- Units: Average power in Megawatts (MW)

Column Mapping:
- Column 0: {PRODUCTION_TYPES_COLUMNS[0]} (Fossil Hard coal)
- Column 1: {PRODUCTION_TYPES_COLUMNS[1]} (Biomass)
"""
with open("production_data_explanation.txt", "w") as f:
    f.write(explanation_text)
print("Created production_data_explanation.txt")

# --- Main execution ---
if __name__ == "__main__":
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Error: API_KEY environment variable not set.")
    else:
        EEP = EntsoeDataProcessor(api_key=api_key)

        start_year = "2025"
        start_date = f"{start_year}-01-01"
        end_date = f"{start_year}-12-31"

        # --- Fetch and save data for DK1 ---
        print("\n--- Processing DK1 Production Data ---")
        dk1_production_data = EEP.fetch_production_by_type_for_range(
            overall_start_date_str=start_date,
            overall_end_date_str=end_date,
            domain_eic=DK1_EIC,
            production_types=PRODUCTION_TYPES_COLUMNS
        )

        if dk1_production_data is not None:
            dk1_filename = f"DK1_production_{start_year}.npy"
            np.save(dk1_filename, dk1_production_data)
            print(f"Successfully saved DK1 production data to {dk1_filename}")
            print(f"Shape of DK1 data: {dk1_production_data.shape}")
        else:
            print("Failed to get DK1 production data.")

        # --- Fetch and save data for DK2 ---
        print("\n--- Processing DK2 Production Data ---")
        dk2_production_data = EEP.fetch_production_by_type_for_range(
            overall_start_date_str=start_date,
            overall_end_date_str=end_date,
            domain_eic=DK2_EIC,
            production_types=PRODUCTION_TYPES_COLUMNS
        )

        if dk2_production_data is not None:
            dk2_filename = f"DK2_production_{start_year}.npy"
            np.save(dk2_filename, dk2_production_data)
            print(f"Successfully saved DK2 production data to {dk2_filename}")
            print(f"Shape of DK2 data: {dk2_production_data.shape}")
        else:
            print("Failed to get DK2 production data.")

        # The next steps would be to get the import/export data, which will use
        # a different API endpoint (likely related to cross-zonal flows).




        