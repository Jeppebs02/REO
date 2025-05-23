import requests
import numpy as np
import xml.etree.ElementTree as ET
import requests
import os
import copy

# <editor-fold desc="API Request">

url = f"https://web-api.tp.entsoe.eu/api?documentType=A73&processType=A16&in_Domain=10Y1001A1001A796&periodStart=202501012200&periodEnd=202501022200&securityToken={os.getenv('API_KEY')}"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)


# </editor-fold>


# <editor-fold desc="pad_hourly_to_15min">

# Namespace URI used in ENTSO-E XML documents
NAMESPACE_URI = 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
# This registration helps in producing cleaner XML output without 'ns0:' prefixes
ET.register_namespace('', NAMESPACE_URI)
# Namespace map for use with find/findall
NS = {'ns': NAMESPACE_URI}

def pad_hourly_to_15min(xml_string: str) -> str | None:
    """
    Pads hourly data in an ENTSO-E GL_MarketDocument XML string to 15-minute intervals.

    For each TimeSeries with a PT60M resolution, this function changes the resolution
    to PT15M and replicates each hourly Point four times, maintaining the original
    quantity (average power) for each new 15-minute Point.

    Args:
        xml_string: A string containing the XML data from the ENTSO-E API.

    Returns:
        A string containing the modified XML data with 15-minute intervals,
        or None if there was an XML parsing error.
    """
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error: {e}")
        # Possibly add error or return None. But for now, just print and return None because I am lazy.
        return None

    # Iterate over all TimeSeries (actual data) elements in the document
    # The './/' means search at any depth from the current element
    for time_series in root.findall('.//ns:TimeSeries', NS):
        # Find "Period" elements within each TimeSeries
        for period in time_series.findall('.//ns:Period', NS):
            # Find the resolution element within the current Period
            # That would mostly be "PT60M"
            resolution_element = period.find('ns:resolution', NS)

            # Check if resolution is PT60M (hourly)
            if resolution_element is not None and resolution_element.text == 'PT60M':
                resolution_element.text = 'PT15M'  # Update resolution text to 15 minutes

                # Collect all original Point elements.
                # Convert to list as we'll be modifying the parent 'period'
                original_points = list(period.findall('ns:Point', NS))

                # Remove all old Point elements from the current Period
                for point_element in original_points:
                    period.remove(point_element)

                # Create and append new 15-minute points
                new_point_position_counter = 1  # Position counter for the new 15-min points. Will go to 4.
                for old_point in original_points:
                    quantity_element = old_point.find('ns:quantity', NS)
                    if quantity_element is not None:
                        quantity_value = quantity_element.text  # The hourly average MW value

                        for _ in range(4):  # Create 4 new 15-min points for each old hourly point
                            # Create new Point element, ensuring it's in the correct namespace
                            new_point_tag = f"{{{NAMESPACE_URI}}}Point"
                            new_point = ET.Element(new_point_tag)

                            # Create and add 'position' sub-element
                            pos_tag = f"{{{NAMESPACE_URI}}}position"
                            pos_el = ET.SubElement(new_point, pos_tag)
                            pos_el.text = str(new_point_position_counter)

                            # Create and add 'quantity' sub-element (using the same hourly average)
                            qty_tag = f"{{{NAMESPACE_URI}}}quantity"
                            qty_el = ET.SubElement(new_point, qty_tag)
                            qty_el.text = quantity_value

                            period.append(new_point)  # Append the new Point to the Period
                            new_point_position_counter += 1

    # Serialize the modified XML tree back to a string
    # xml_declaration=True adds the <?xml version="1.0" encoding="UTF-8"?> header
    modified_xml_string = ET.tostring(root, encoding='unicode', xml_declaration=True)
    return modified_xml_string
# </editor-fold>

# <editor-fold desc="Seperate XML To each PSR">

def extract_psr_data_to_xml(xml_string: str, psr_name: str) -> str | None:
    """
    Extracts the TimeSeries data for a specific PSR (by name) from a
    GL_MarketDocument XML string and returns it as a new XML string
    containing only that PSR's data along with the original document header.

    Args:
        xml_string: A string containing the XML data (potentially padded).
        psr_name: The name of the Power System Resource (e.g., "Anholt")
                  to extract.

    Returns:
        A string containing the new XML document with only the specified PSR's
        TimeSeries data and the original header, or None if the PSR is not found
        or an XML parsing error occurs.
    """
    try:
        # Parse the original XML string into an ElementTree object
        original_root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error: {e}")
        return None

    target_timeseries_element = None
    for ts_element in original_root.findall('.//ns:TimeSeries', NS):
        name_element = ts_element.find('.//ns:MktPSRType/ns:PowerSystemResources/ns:name', NS)
        if name_element is not None and name_element.text == psr_name:
            target_timeseries_element = ts_element
            break  # Found the PSR, no need to search further :)

    if target_timeseries_element is None:
        print(f"PSR with name '{psr_name}' not found in the XML.")
        return None

    # Create a new root for the output XML document
    new_root_tag = f"{{{NAMESPACE_URI}}}GL_MarketDocument"
    new_root = ET.Element(new_root_tag)

    # Copy attributes from the original root to the new root, if any
    # (e.g., xmlns if not handled by register_namespace during serialization)
    for key, value in original_root.attrib.items():
        new_root.set(key, value)

    # List of header tags that are direct children of GL_MarketDocument
    # These should also be copied to the new document.
    header_tags = [
        'mRID', 'revisionNumber', 'type', 'process.processType',
        'sender_MarketParticipant.mRID', 'sender_MarketParticipant.marketRole.type',
        'receiver_MarketParticipant.mRID', 'receiver_MarketParticipant.marketRole.type',
        'createdDateTime', 'time_Period.timeInterval'
    ]

    for tag_name in header_tags:
        # Construct the fully qualified tag name for find
        namespaced_tag = f'ns:{tag_name}'
        header_element = original_root.find(namespaced_tag, NS)
        if header_element is not None:
            new_root.append(copy.deepcopy(header_element))  # Append a copy of the header element

    # Append a deep copy of the found TimeSeries element to the new root
    new_root.append(copy.deepcopy(target_timeseries_element))

    # Serialize the new XML tree (with only the specific PSR's TimeSeries) to a string
    # The encoding='unicode' makes ET.tostring return a string directly.
    # xml_declaration=True adds the <?xml version="1.0" encoding="UTF-8"?>
    filtered_xml_string = ET.tostring(new_root, encoding='unicode', xml_declaration=True)

    return filtered_xml_string

# </editor-fold>


# <editor-fold desc="Single PSR XML to Numpy Array">

#VERY IMPORTANT, THIS FUNCTION ASSUMES THAT THERE IS ONLY ONE PSR IN THE XML DOCUMENT
def psr_xml_to_numpy(psr_xml_string: str) -> np.ndarray | None:
    """
    Converts a padded and extracted PSR XML string (containing a single TimeSeries)
    into a 2D NumPy array where each row is [position, quantity].

    Args:
        psr_xml_string: An XML string containing the data for a single
                        Power System Resource. It's assumed this XML
                        has one TimeSeries element.

    Returns:
        A 2D NumPy array with columns for 'position' and 'quantity',
        or None if parsing fails, no TimeSeries is found, or no Points are found.
        The dtype of the array will be float to accommodate quantities.
    """
    try:
        root = ET.fromstring(psr_xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error: {e}")
        return None

    # Find the TimeSeries element. Since the XML is pre-filtered for one PSR,
    # there should be exactly one.
    time_series_element = root.find('.//ns:TimeSeries', NS)
    if time_series_element is None:
        print("Error: No TimeSeries element found in the provided XML.")
        return None

    period_element = time_series_element.find('.//ns:Period', NS)
    if period_element is None:
        print("Error: No Period element found within the TimeSeries.")
        return None

    points_data = []
    for point_element in period_element.findall('ns:Point', NS):
        position_el = point_element.find('ns:position', NS)
        quantity_el = point_element.find('ns:quantity', NS)

        if position_el is not None and quantity_el is not None:
            try:
                position = int(position_el.text)
                quantity = float(quantity_el.text)  # Use float for quantity
                points_data.append([position, quantity])
            except (ValueError, TypeError) as e:
                print(
                    f"Warning: Could not parse point data: {position_el.text}, {quantity_el.text}. Error: {e}. Skipping point.")
                continue  # Skip this point if parsing fails
        else:
            print("Warning: Point element missing position or quantity. Skipping point.")

    if not points_data:
        print("No valid points found to create a NumPy array.")
        return None

    # Convert the list of lists to a NumPy array
    # Using float as dtype to accommodate potential float quantities
    numpy_array = np.array(points_data, dtype=float)

    return numpy_array


# </editor-fold>






# <editor-fold desc="Misc">

# </editor-fold>