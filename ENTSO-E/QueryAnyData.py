from entsoe import EntsoeRawClient
from entsoe import EntsoePandasClient
import pandas as pd
import os


client = EntsoePandasClient(api_key="791704c1-52a0-41f8-b30e-ad4b7e849c7a")

start = pd.Timestamp('20250101', tz='Europe/Copenhagen')
end = pd.Timestamp('20251231', tz='Europe/Copenhagen')
# Second end date
end2 = pd.Timestamp('20250220', tz='Europe/Copenhagen')

country_code = 'DK'  # Denmark
process_type = 'A16'  # Realized
domain_eic = "10Y1001A1001A796"

# EIC codes https://transparencyplatform.zendesk.com/hc/en-us/articles/15885757676308-Area-List-with-Energy-Identification-Code-EIC



#client.query_generation_per_plant(country_code, start=start, end=end, psr_type=None, include_eic=False)
data = client.query_generation_per_plant(domain_eic, start=start, end=end, psr_type=None, include_eic=True)
data.to_csv('outfile.csv')






