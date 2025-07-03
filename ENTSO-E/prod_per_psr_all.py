import os
import numpy as np
from EntsoEDataProcessor import EntsoeDataProcessor  # Make sure to import your class



PSR_TYPE_MAPPING = {
    # FOSSIL FUELS
    'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal-derived gas',
    'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal',
    'B06': 'Fossil Oil',
    'B07': 'Fossil Oil shale',
    'B08': 'Fossil Peat',
    # RENEWABLES
    'B01': 'Biomass',
    'B09': 'Geothermal',
    'B11': 'Hydro Run-of-river and poundage', # B11 is Run-of-river
    'B12': 'Hydro Water Reservoir',           # B12 is Reservoir
    'B13': 'Marine',
    'B15': 'Other renewable',
    'B16': 'Solar',
    'B18': 'Wind Offshore',
    'B19': 'Wind Onshore',
    # OTHER / STORAGE
    'B10': 'Hydro Pumped Storage', # B10 is Pumped Storage
    'B14': 'Nuclear',              # B14 is Nuclear. We don't have any :(
    'B17': 'Waste',
    'B20': 'Other',
    'B25': 'Energy storage'
}

# This list will be passed to the fetch function and defines the column order.
# The order is based on the dictionary above.
PRODUCTION_TYPES_COLUMNS = list(PSR_TYPE_MAPPING.keys())

# Define the bidding zone EICs
DK1_EIC = "10YDK-1--------W"
DK2_EIC = "10YDK-2--------M"



# --- Main execution ---
if __name__ == "__main__":
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Error: API_KEY environment variable not set.")
    else:
        EEP = EntsoeDataProcessor(api_key=api_key)

        start_year = "2024"
        start_date = f"{start_year}-01-01"
        end_date = f"{start_year}-12-31"

        # --- Fetch and save data for DK1 ---
        print("\n--- Processing DK1 Production Data for ALL Types ---")
        dk1_production_data = EEP.fetch_production_by_type_for_range(
            overall_start_date_str=start_date,
            overall_end_date_str=end_date,
            domain_eic=DK1_EIC,
            production_types=PRODUCTION_TYPES_COLUMNS  # Pass the new, comprehensive list
        )

        if dk1_production_data is not None:
            dk1_filename = f"DK1_production_all_types_{start_year}.npy"
            np.save(dk1_filename, dk1_production_data)
            print(f"Successfully saved DK1 production data to {dk1_filename}")
            print(f"Shape of DK1 data: {dk1_production_data.shape}")
        else:
            print("Failed to get DK1 production data.")

        # --- Fetch and save data for DK2 ---
        print("\n--- Processing DK2 Production Data for ALL Types ---")
        dk2_production_data = EEP.fetch_production_by_type_for_range(
            overall_start_date_str=start_date,
            overall_end_date_str=end_date,
            domain_eic=DK2_EIC,
            production_types=PRODUCTION_TYPES_COLUMNS  # Pass the new, comprehensive list
        )

        if dk2_production_data is not None:
            dk2_filename = f"DK2_production_all_types_{start_year}.npy"
            np.save(dk2_filename, dk2_production_data)
            print(f"Successfully saved DK2 production data to {dk2_filename}")
            print(f"Shape of DK2 data: {dk2_production_data.shape}")
        else:
            print("Failed to get DK2 production data.")



    # --- Generate detailed explanation file ---

    print("\n--- Generating Detailed Explanation File ---")

    # Determine the shape information for the explanation text
    dk1_shape_info = str(dk1_production_data.shape) if dk1_production_data is not None else "FAILED_TO_FETCH"
    dk2_shape_info = str(dk2_production_data.shape) if dk2_production_data is not None else "FAILED_TO_FETCH"

    explanation_lines = [
        f"--- Data Explanation for ENTSO-E Production Data ({start_year}) ---",
        "This file describes the content and structure of the generated NumPy (.npy) files.",
        "\n--- General Information ---",
        "- File format: NumPy array (.npy)",
        "- Time resolution: 15-minute intervals.",
        "- Units: Average power in Megawatts (MW)",
        f"- Data Period: {start_date} to {end_date}",
        "\n--- Generated Files and Array Shapes ---",
        f"- DK1_production_all_types_{start_year}.npy: Shape = {dk1_shape_info}",
        f"- DK2_production_all_types_{start_year}.npy: Shape = {dk2_shape_info}",
        "\n--- Production Data Column Mapping ---"
    ]

    for i, code in enumerate(PRODUCTION_TYPES_COLUMNS):
        name = PSR_TYPE_MAPPING.get(code, 'Unknown Type')
        explanation_lines.append(f"- Column {i}: {code} ({name})")

    explanation_text = "\n".join(explanation_lines)

    with open("production_data_explanation.txt", "w") as f:
        f.write(explanation_text)

    print("Created/Updated production_data_explanation.txt with shape information.")