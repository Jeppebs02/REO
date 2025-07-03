import os
import numpy as np
from EntsoEDataProcessor import EntsoeDataProcessor

# --- EIC Codes ---
DE_LU_EIC = "10Y1001A1001A82H"
DK1_EIC = "10YDK-1--------W"
DK2_EIC = "10YDK-2--------M"

# Create the explanation file for transport data
transport_explanation = f"""
Data columns for DE_DK1_flow.npy and DE_DK2_flow.npy:
- File format: NumPy array (.npy)
- Time resolution: 15-minute intervals.
- Units: Average power in Megawatts (MW)

Column Mapping:
- Column 0: Import to DK (Flow from DE-LU to DK)
- Column 1: Export from DK (Flow from DK to DE-LU)
"""
with open("transport_data_explanation.txt", "w") as f:
    f.write(transport_explanation)
print("Created transport_data_explanation.txt")

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

        # --- Process DE <-> DK1 Flow ---
        print("\n--- Processing DE <-> DK1 Physical Flow ---")
        # Column 0: Import to DK1 (from DE-LU)
        import_to_dk1 = EEP.fetch_physical_flow_for_range(start_date, end_date, DE_LU_EIC, DK1_EIC)
        # Column 1: Export from DK1 (to DE-LU)
        export_from_dk1 = EEP.fetch_physical_flow_for_range(start_date, end_date, DK1_EIC, DE_LU_EIC)

        if import_to_dk1 is not None and export_from_dk1 is not None:
            # Stack the two 1D arrays as columns into a 2D array
            de_dk1_flow_data = np.stack((import_to_dk1, export_from_dk1), axis=1)
            filename = f"DE_DK1_flow_{start_year}.npy"
            np.save(filename, de_dk1_flow_data)
            print(f"Successfully saved DE-DK1 flow data to {filename}")
            print(f"Shape of DE-DK1 flow data: {de_dk1_flow_data.shape}")
        else:
            print("Failed to get DE-DK1 flow data.")

        # --- Process DE <-> DK2 Flow ---
        print("\n--- Processing DE <-> DK2 Physical Flow ---")
        # Column 0: Import to DK2 (from DE-LU)
        import_to_dk2 = EEP.fetch_physical_flow_for_range(start_date, end_date, DE_LU_EIC, DK2_EIC)
        # Column 1: Export from DK2 (to DE-LU)
        export_from_dk2 = EEP.fetch_physical_flow_for_range(start_date, end_date, DK2_EIC, DE_LU_EIC)

        if import_to_dk2 is not None and export_from_dk2 is not None:
            # Stack the two 1D arrays as columns into a 2D array
            de_dk2_flow_data = np.stack((import_to_dk2, export_from_dk2), axis=1)
            filename = f"DE_DK2_flow_{start_year}.npy"
            np.save(filename, de_dk2_flow_data)
            print(f"Successfully saved DE-DK2 flow data to {filename}")
            print(f"Shape of DE-DK2 flow data: {de_dk2_flow_data.shape}")
        else:
            print("Failed to get DE-DK2 flow data.")