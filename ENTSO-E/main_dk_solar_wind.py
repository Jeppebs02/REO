import os
from time import sleep

from EntsoEDataProcessor import EntsoeDataProcessor



EEP = EntsoeDataProcessor(os.getenv("API_KEY"))

# Variables
# Dates are in YYYY-MM-DD format
start_date = "2024-01-01"
end_date = "2025-05-24"
domain_eic = "10Y1001A1001A796"
psr_name_1 = "Solar Park Gedmosen"
psr_name_2 = "Solar Park Holsted"

gedmose_np_array = EEP.fetch_and_process_psr_data_range(start_date, end_date, domain_eic, psr_name_1)

print("Sleeping for 1 minute to avoid rate limiting...")
sleep(60)  # Optional: Sleep to avoid rate limiting

holsted_np_array = EEP.fetch_and_process_psr_data_range(start_date, end_date, domain_eic, psr_name_2)

print("Arrays fetched and processed successfully.")