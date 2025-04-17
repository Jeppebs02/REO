from entsoe import EntsoeRawClient
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import xmltodict
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
import json
import time
import os

# Get the current date
current_date = datetime.now()

# Get the day of the year
day_of_year = current_date.timetuple().tm_yday-193

#Get the current day in YYYYMMDD format
current_date_timestamp_end = current_date.strftime("%Y-%m-%d")

# Remove unwanted characters from the formatted date
current_date_timestamp_end = current_date_timestamp_end.replace("-", "")


#Calculate yesterdays date
current_date_timestamp_start= current_date - timedelta(days=1)

# Format yesterday's date as a string
current_date_timestamp_start= current_date_timestamp_start.strftime("%Y-%m-%d")

#Remove unwanted characters from the formatted date
current_date_timestamp_start= current_date_timestamp_start.replace("-","")

print(current_date_timestamp_start)

client = EntsoeRawClient(api_key= os.getenv("API_KEY"))

#The timetamp syntax is YYYYMMDD
startdk = pd.Timestamp("20230101", tz='Europe/Copenhagen')
enddk = pd.Timestamp("20230102", tz='Europe/Copenhagen')

startfi= pd.Timestamp("20230101", tz='Europe/Helsinki')
endfi= pd.Timestamp("20230102", tz='Europe/Helsinki')


country_code = 'DK'  # Denmark
country_code2="FI"   # Finalnd
country_code_from = ''
country_code_to = ''
type_marketagreement_type = 'A01' #Daily data
contract_marketagreement_type = 'A01' #Daily data


#Calculate quantity sum
def calculate_quantity_sum(data, key='quantity'):
    total_sum = 0
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key:
                total_sum += int(v)
            elif isinstance(v, (dict, list)):
                total_sum += calculate_quantity_sum(v, key)
    elif isinstance(data, list):
        for item in data:
            total_sum += calculate_quantity_sum(item, key)
    return total_sum

#Save data function
def save_data(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file)

#Load data function
def load_data(filename):
    with open(filename) as file:
        data = json.load(file)
    return data

#Denmark offshore + onshore + solar
xml_string = client.query_generation(country_code, startdk, enddk, psr_type="B18")  #B18 Offshore wind
xml_string2= client.query_generation(country_code, startdk, enddk, psr_type="B19")  #B19 Onshore wind
xml_string4= client.query_generation(country_code, startdk, enddk, psr_type="B16")  #B16 Solar

Solar=xmltodict.parse(xml_string4)
WindOffShore=xmltodict.parse(xml_string)
WindOnShore=xmltodict.parse(xml_string2)

#Calculate the total sum of Solar and Wind.
SolarSum= calculate_quantity_sum(Solar)
WindOffShoreSum= calculate_quantity_sum(WindOffShore)
WindOnShoreSum= calculate_quantity_sum(WindOnShore)

#Add them all together to get the total sum of energy for Denmark.
Total_DenmarkSum=SolarSum+WindOffShoreSum+WindOnShoreSum

# Print TotalDenmarkSum
print(str(round(Total_DenmarkSum/1000, 1))+" GWh")

#Format the dictionaries from earlier (xmltodict parse) into one big dictionary with all the combined values of danish energy production
Total_Denmark = {}
for point in Solar['GL_MarketDocument']['TimeSeries']['Period']['Point']:
    position = point['position']
    solar_quantity = int(point['quantity'])
    wind_onshore_quantity = int(WindOnShore['GL_MarketDocument']['TimeSeries']['Period']['Point'][int(position) - 1]['quantity'])
    wind_offshore_quantity = int(WindOffShore['GL_MarketDocument']['TimeSeries']['Period']['Point'][int(position) - 1]['quantity'])
    total_quantity = solar_quantity + wind_onshore_quantity + wind_offshore_quantity
    Total_Denmark[position] = {position: total_quantity}

#Remove nesting elements from the combined dictionary
Total_Denmark = {int(key): value[key] for key, value in Total_Denmark.items()}
#Turn dict into tuple
Total_Denmark=[(day_of_year, key, value) for key, value in Total_Denmark.items()]

#Print the array of combined energy production from Denmark
print(f"Denmark: {Total_Denmark}")


#Finland data.

xml_string3= client.query_generation(country_code2, startfi, endfi, psr_type="B14") #B14 Nuclear

#Format the xml into a dict
Nuclear = xmltodict.parse(xml_string3)

#Calculate the total sum of Finnish nuclear and divide by 4 to get it pr hr.
Total_FinlandSum = calculate_quantity_sum(Nuclear)/4

#Print the total sum in GWh rounded to the first decimal
print(str(round(Total_FinlandSum/1000, 1))+ " GWh")

#Make new dict for Finland data array
Total_Finland= {}

#Remove nesting and print the array
points_Finland=Nuclear['GL_MarketDocument']['TimeSeries']['Period']['Point']
for point in points_Finland:
    position = point['position']
    positionint=int(position)
    quantity = point['quantity']
    Total_Finland[positionint/4] = int(quantity)
    
Total_Finland=[(day_of_year, key, value) for key, value in Total_Finland.items()]
print(f"Finland: {Total_Finland}")

save_data(Total_Denmark,"DenmarkData")
save_data(Total_Finland,"FinlandData")

