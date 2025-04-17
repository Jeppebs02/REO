from entsoe import EntsoeRawClient
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import xmltodict



#WIP data format file, implement in the MainEnergy file once complete
with open('C:\\Users\\jeppe\\Documents\\GitHub\\Programming-testing\\FInuclear.xml', 'r') as file:
    xml_data = file.read()

data_dict = xmltodict.parse(xml_data)

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

total_quantity_sum_MWh = calculate_quantity_sum(data_dict)/4
print(total_quantity_sum_MWh)