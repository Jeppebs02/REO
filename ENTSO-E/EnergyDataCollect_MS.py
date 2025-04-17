from typing import List, Any

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

client = EntsoeRawClient(api_key= os.getenv("API_KEY"))

country_code = 'DK'  # Denmark
country_code2 = "FI"   # Finalnd
type_marketagreement_type = 'A01' #Daily data
contract_marketagreement_type = 'A01' #Daily data

def find_quantity_values(xml_string):
    root = ET.fromstring(xml_string)  # Parse the XML string
    values = []

    # Define the XML namespace used in the document
    namespace = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}

    for point in root.findall('.//ns:Point', namespace):
        quantity_element = point.find('ns:quantity', namespace)
        if quantity_element is not None and quantity_element.text is not None:
            values.append(quantity_element.text)

    return values

def save_data(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file)

#Solar=np.zeros((365*24,1))
#Onshore=np.zeros((365*24,1))
#Offshore=np.zeros((365*24,1))
#Nuclear=np.zeros((365*24*4,1))

Solar = []
Onshore = []
Offshore = []
Nuclear = []

k=0
start_date = datetime.strptime('20230101', '%Y%m%d')
#end_date = datetime.strptime('20230720', '%Y%m%d')
end_date = datetime.strptime('20231020', '%Y%m%d')

current_date = start_date

while current_date <= end_date:

    #The timetamp syntax is YYYYMMDD
    startdk = pd.Timestamp(str(current_date), tz='Europe/Copenhagen')
    enddk = pd.Timestamp(str(current_date+timedelta(days=1)), tz='Europe/Copenhagen')

    #Denmark offshore + onshore + solar
    xml_string_DK_Offshore=client.query_generation(country_code, startdk, enddk, psr_type="B18")  #B18 Offshore wind
    xml_string_DK_Onshore= client.query_generation(country_code, startdk, enddk, psr_type="B19")  #B19 Onshore wind
    xml_string_DK_Solar= client.query_generation(country_code, startdk, enddk, psr_type="B16")  #B16 Solar

    temp_Solar = find_quantity_values(xml_string_DK_Solar)
    temp_Onshore = find_quantity_values(xml_string_DK_Onshore)
    temp_Offshore = find_quantity_values(xml_string_DK_Offshore)

    Solar = np.append(Solar, temp_Solar)
    Onshore = np.append(Onshore, temp_Onshore)
    Offshore = np.append(Offshore, temp_Offshore)

    current_date += timedelta(days=1)
    print(k)
    k = k + 1
    print(len(Solar))
    time.sleep(1)

# 3. juli mellem 21 og 22 er tom. Dag 184 Det skal adderes.
k=0
start_date = datetime.strptime('20230101', '%Y%m%d')
end_date = datetime.strptime('20230520', '%Y%m%d')

current_date = start_date

while current_date <= end_date:

    #The timetamp syntax is YYYYMMDD
    startfi= pd.Timestamp(str(current_date), tz='Europe/Helsinki')
    endfi= pd.Timestamp(str(current_date+timedelta(days=1)), tz='Europe/Helsinki')

    #Denmark offshore + onshore + solar
    xml_string_FI_Nuclear = client.query_generation(country_code2, startfi, endfi, psr_type="B14")  # B14 Nuclear

    temp_Nuclear = find_quantity_values(xml_string_FI_Nuclear)

    Nuclear = np.append(Nuclear, np.repeat(temp_Nuclear, 4))

    current_date += timedelta(days=1)
    print(k)
    print(len(Nuclear))
    k = k + 1
    time.sleep(1)

end_date = datetime.strptime('20231020', '%Y%m%d')

while current_date <= end_date:

    #The timetamp syntax is YYYYMMDD
    startfi= pd.Timestamp(str(current_date), tz='Europe/Helsinki')
    endfi= pd.Timestamp(str(current_date+timedelta(days=1)), tz='Europe/Helsinki')

    #Finland Nuclear
    xml_string_FI_Nuclear = client.query_generation(country_code2, startfi, endfi, psr_type="B14")  # B14 Nuclear

    temp_Nuclear = find_quantity_values(xml_string_FI_Nuclear)

    Nuclear = np.append(Nuclear, temp_Nuclear)

    current_date += timedelta(days=1)
    print(k)
    print(len(Nuclear))
    k = k + 1
    time.sleep(1)


np.save('Solar.npy', Solar)
np.save('Onshore.npy', Onshore)
np.save('Offshore.npy', Offshore)
np.save('Nuclear.npy', Nuclear)

#save_data(Solar,"Solar")
#save_data(Onshore,"Onshore")
#save_data(Offshore,"Offshore")
#save_data(Nuclear,"Nuclear")