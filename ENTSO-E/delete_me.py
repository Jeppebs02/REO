import os

from entsoe import EntsoePandasClient
import pandas as pd

client = EntsoePandasClient(api_key=os.getenv("API_KEY"))

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'DK'  # Belgium
country_code_from = 'FR'  # France
country_code_to = 'DE_LU' # Germany-Luxembourg
type_marketagreement_type = 'A01'
contract_marketagreement_type = "A01"
process_type = 'A51'

# EIC codes https://transparencyplatform.zendesk.com/hc/en-us/articles/15885757676308-Area-List-with-Energy-Identification-Code-EIC



data = client.query_generation_per_plant("10Y1001A1001A796", start=start, end=end, psr_type=None, include_eic=False)

data.to_csv('outfile.csv')