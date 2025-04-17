from entsoe import EntsoeRawClient
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import os

client = EntsoeRawClient(api_key= os.getenv("API_KEY"))

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium
country_code_from = 'FR'  # France
country_code_to = 'DE_LU' # Germany-Luxembourg
type_marketagreement_type = 'A01'
contract_marketagreement_type = 'A01'

# methods that return XML
client.query_day_ahead_prices(country_code, start, end)
client.query_net_position(country_code, start, end, dayahead=True)
client.query_load(country_code, start, end)
client.query_load_forecast(country_code, start, end)
client.query_wind_and_solar_forecast(country_code, start, end, psr_type=None)
client.query_generation_forecast(country_code, start, end)
client.query_generation(country_code, start, end, psr_type=None)
client.query_generation_per_plant(country_code, start, end, psr_type=None)
client.query_installed_generation_capacity(country_code, start, end, psr_type=None)
client.query_installed_generation_capacity_per_unit(country_code, start, end, psr_type=None)
client.query_crossborder_flows(country_code_from, country_code_to, start, end)
client.query_scheduled_exchanges(country_code_from, country_code_to, start, end, dayahead=False)
client.query_net_transfer_capacity_dayahead(country_code_from, country_code_to, start, end)
client.query_net_transfer_capacity_weekahead(country_code_from, country_code_to, start, end)
client.query_net_transfer_capacity_monthahead(country_code_from, country_code_to, start, end)
client.query_net_transfer_capacity_yearahead(country_code_from, country_code_to, start, end)
client.query_intraday_offered_capacity(country_code_from, country_code_to, start, end, implicit=True)
client.query_offered_capacity(country_code_from, country_code_to, start, end, contract_marketagreement_type, implicit=True)
client.query_contracted_reserve_prices(country_code, start, end, type_marketagreement_type, psr_type=None)
client.query_contracted_reserve_amount(country_code, start, end, type_marketagreement_type, psr_type=None)
client.query_aggregate_water_reservoirs_and_hydro_storage(country_code, start, end)


xml_string = client.query_day_ahead_prices(country_code, start, end)
with open('outfile.xml', 'w') as f:
    f.write(xml_string)



#Blueprints for data collection