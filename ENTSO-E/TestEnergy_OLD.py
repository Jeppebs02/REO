#use other files



import os
import math
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

import pandas as pd
from entsoe import EntsoePandasClient
import os


client = EntsoePandasClient(api_key= os.getenv("API_KEY"))

start = pd.Timestamp('20201201', tz ='UTC')
end = pd.Timestamp('20201202', tz ='UTC')
country_code_1 = 'DK'  # 
country_code_2 = 'FI'  # 
country_code_3 = ''  # 

#day-ahead market prices (€/MWh)
DA_prices = client.query_day_ahead_prices(country_code_1, start=start,end=end)

#generation (MW)
generation = client.query_generation(country_code_1, start=start,end=end)
generation_per_plant = client.query_generation_per_plant(country_code_1, start=start,end=end)
generation_forecast = client.query_generation_forecast(country_code_1, start=start,end=end)
wind_solar_forecast = client.query_wind_and_solar_forecast(country_code_1, start=start,end=end, psr_type=None)
installed_generation_capacity = client.query_installed_generation_capacity(country_code_1, start=start,end=end)
installed_generation_capacity_per_unit = client.query_installed_generation_capacity_per_unit(country_code_1, start=start,end=end)

#load and load forecast (MW)
load = client.query_load(country_code_1, start=start,end=end)
load_forecast = client.query_load_forecast(country_code_1, start=start,end=end)

#day-ahead scheduled (commercial) exchanges (MW)
scheduled_exchanges = client.query_scheduled_exchanges(country_code_1, country_code_2, start=start,end=end)

#cross-border flows (physical) (MW): to get resulting flow both directions need to be considerd, e.g netflow_AT_DE = (AT-DE) - (DE-AT) 
crossborder_flows_1 = client.query_crossborder_flows(country_code_1, country_code_2, start=start,end=end) 
crossborder_flows_2 = client.query_crossborder_flows( country_code_2,country_code_1, start=start,end=end) 
crossborder_flow_net = crossborder_flows_1 - crossborder_flows_2

#works only for countries without flow-based border (MW)
net_transfer_capacity_dayahead = client.query_net_transfer_capacity_dayahead(country_code_1, country_code_3, start=start,end=end)
net_transfer_capacity_monthahead = client.query_net_transfer_capacity_monthahead(country_code_1, country_code_3, start=start,end=end)
net_transfer_capacity_weekahead = client.query_net_transfer_capacity_weekahead(country_code_1, country_code_3, start=start,end=end)
net_transfer_capacity_yearahead = client.query_net_transfer_capacity_yearahead(country_code_1, country_code_3, start=start,end=end)

#contracted reserves (MW) and prices (€/MW/period)
contracted_reserve_amount = client.query_contracted_reserve_amount(country_code_1, start=start, end=end, type_marketagreement_type='A01')
contracted_reserve_prices = client.query_contracted_reserve_prices(country_code_1, start=start, end=end, type_marketagreement_type='A01')

#unavailability of generation and production units
unavailability_of_generation_units = client.query_unavailability_of_generation_units(country_code_1, start=start,end=end)
unavailability_of_production_units = client.query_unavailability_of_production_units(country_code_1, start=start,end=end)