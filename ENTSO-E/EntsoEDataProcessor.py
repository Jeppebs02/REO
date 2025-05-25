import time
import requests
import numpy as np
import xml.etree.ElementTree as ET
import os # Might need it later.
import copy
from datetime import date, timedelta, datetime

class EntsoeDataProcessor:
    # Namespace URI used in ENTSO-E XML documents
    NAMESPACE_URI = 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
    # Namespace map for use with find/findall
    NS = {'ns': NAMESPACE_URI}

    # Rate Limiting Constants
    MAX_REQUESTS_PER_MINUTE = 400
    RATE_LIMIT_BUFFER = 10  # Start sleeping when count reaches MAX_REQUESTS_PER_MINUTE - RATE_LIMIT_BUFFER
    RATE_LIMIT_WINDOW_SECONDS = 60.0  # Duration of the rate limit window in seconds
    GENERAL_POLITENESS_SECONDS = 0.5 # General sleep between requests

    def __init__(self, api_key: str):
        self.api_key = api_key
        # This registration helps in producing cleaner XML output without 'ns0:' prefixes
        ET.register_namespace('', self.NAMESPACE_URI)

        # Rate Limiting State Variables
        self.request_count_this_minute = 0
        self.minute_window_start_time = datetime.now()



    # <editor-fold desc="pad_hourly_to_15min">
    def pad_hourly_to_15min(self, xml_string: str) -> str | None:
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
        for time_series in root.findall('.//ns:TimeSeries', self.NS):
            # Find "Period" elements within each TimeSeries
            for period in time_series.findall('.//ns:Period', self.NS):
                # Find the resolution element within the current Period
                # That would mostly be "PT60M"
                resolution_element = period.find('ns:resolution', self.NS)

                # Check if resolution is PT60M (hourly)
                if resolution_element is not None and resolution_element.text == 'PT60M':
                    resolution_element.text = 'PT15M'  # Update resolution text to 15 minutes

                    # Collect all original Point elements.
                    # Convert to list as we'll be modifying the parent 'period'
                    original_points = list(period.findall('ns:Point', self.NS))

                    # Remove all old Point elements from the current Period
                    for point_element in original_points:
                        period.remove(point_element)

                    # Create and append new 15-minute points
                    new_point_position_counter = 1  # Position counter for the new 15-min points. Will go to 4.
                    for old_point in original_points:
                        quantity_element = old_point.find('ns:quantity', self.NS)
                        if quantity_element is not None:
                            quantity_value = quantity_element.text  # The hourly average MW value

                            for _ in range(4):  # Create 4 new 15-min points for each old hourly point
                                # Create new Point element, ensuring it's in the correct namespace
                                new_point_tag = f"{{{self.NAMESPACE_URI}}}Point"
                                new_point = ET.Element(new_point_tag)

                                # Create and add 'position' sub-element
                                pos_tag = f"{{{self.NAMESPACE_URI}}}position"
                                pos_el = ET.SubElement(new_point, pos_tag)
                                pos_el.text = str(new_point_position_counter)

                                # Create and add 'quantity' sub-element (using the same hourly average)
                                qty_tag = f"{{{self.NAMESPACE_URI}}}quantity"
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
    def extract_psr_data_to_xml(self, xml_string: str, psr_name: str) -> str | None:
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
        for ts_element in original_root.findall('.//ns:TimeSeries', self.NS):
            name_element = ts_element.find('.//ns:MktPSRType/ns:PowerSystemResources/ns:name', self.NS)
            if name_element is not None and name_element.text == psr_name:
                target_timeseries_element = ts_element
                break  # Found the PSR, no need to search further :)

        if target_timeseries_element is None:
            print(f"PSR with name '{psr_name}' not found in the XML.")
            return None

        # Create a new root for the output XML document
        new_root_tag = f"{{{self.NAMESPACE_URI}}}GL_MarketDocument"
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
            header_element = original_root.find(namespaced_tag, self.NS)
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
    def psr_xml_to_numpy(self, psr_xml_string: str) -> np.ndarray | None:
        """
        Converts a padded and extracted PSR XML string (containing a single TimeSeries)
        into a 2D NumPy array where each row is [YYYYMMDDHH_string, quantity].

        Args:
            psr_xml_string: An XML string containing the data for a single
                            Power System Resource. It's assumed this XML
                            has one TimeSeries element and that the points
                            represent 15-minute intervals (due to prior padding).

        Returns:
            A 2D NumPy array with columns for 'YYYYMMDDHH' and 'quantity',
            or None if parsing fails, no TimeSeries is found, or no Points are found.
            The dtype of the array will be object.
        """
        try:
            root = ET.fromstring(psr_xml_string)
        except ET.ParseError as e:
            print(f"XML Parsing Error in psr_xml_to_numpy: {e}")
            return None

        time_series_element = root.find('.//ns:TimeSeries', self.NS)
        if time_series_element is None:
            print("Error: No TimeSeries element found in the provided XML.")
            return None

        period_element = time_series_element.find('.//ns:Period', self.NS)
        if period_element is None:
            print("Error: No Period element found within the TimeSeries.")
            return None

        # Get the start time of the period from the XML
        period_start_time_str_el = period_element.find('.//ns:timeInterval/ns:start', self.NS)
        if period_start_time_str_el is None or not period_start_time_str_el.text:
            print("Error: Could not find or parse <Period><timeInterval><start> from XML.")
            return None

        try:
            # Parse the ISO 8601 datetime string. Example: "2025-03-11T22:00Z"
            # The 'Z' indicates UTC. Python's fromisoformat handles this directly
            # if the 'Z' is replaced by '+00:00' or if it's Python 3.11+
            # For broader compatibility, let's handle 'Z' explicitly.
            period_start_time_str = period_start_time_str_el.text
            if period_start_time_str.endswith('Z'):
                period_start_time_str = period_start_time_str[:-1] + "+00:00"
            period_start_datetime = datetime.fromisoformat(period_start_time_str)
        except ValueError as e:
            print(f"Error: Could not parse period start datetime string '{period_start_time_str_el.text}': {e}")
            return None

        points_data = []
        # The 'position' from the Point element is for the 15-minute intervals
        # after padding. It will range from 1 to 96 for a 24-hour period.
        for point_element in period_element.findall('ns:Point', self.NS):
            position_el = point_element.find('ns:position', self.NS)
            quantity_el = point_element.find('ns:quantity', self.NS)

            if position_el is not None and quantity_el is not None:
                try:
                    point_position_15min = int(position_el.text)  # 1-based index for 15-min interval
                    quantity = float(quantity_el.text)

                    # Calculate which hour this 15-minute interval belongs to.
                    # (point_position_15min - 1) makes it 0-indexed.
                    # // 4 gives the 0-indexed hour offset from the period_start_datetime.
                    hour_offset_from_period_start = (point_position_15min - 1) // 4

                    # Calculate the actual start datetime of the hour for this point
                    actual_hour_start_dt = period_start_datetime + timedelta(hours=hour_offset_from_period_start)

                    # Format this datetime as YYYYMMDDHH
                    formatted_timestamp = actual_hour_start_dt.strftime("%Y%m%d%H")

                    points_data.append([formatted_timestamp, quantity])

                except (ValueError, TypeError) as e:
                    print(
                        f"Warning: Could not parse point data: pos='{position_el.text}', qty='{quantity_el.text}'. Error: {e}. Skipping point.")
                    continue
            else:
                print("Warning: Point element missing position or quantity. Skipping point.")

        if not points_data:
            # print("No valid points found to create a NumPy array.") # Can be noisy
            return None

        # Convert the list of lists to a NumPy array
        # Using dtype=object because the first column is a string (timestamp)
        # and the second is a float (quantity).
        numpy_array = np.array(points_data, dtype=object)

        return numpy_array
    # </editor-fold>

    # <editor-fold desc="fetch_and_process_psr_data_range">
    def fetch_and_process_psr_data_range(
            self,
            overall_start_date_str: str,  # "yyyy-mm-dd"
            overall_end_date_str: str,  # "yyyy-mm-dd"
            domain_eic: str,
            psr_name_to_extract: str,
            base_api_url: str = "https://web-api.tp.entsoe.eu/api",
            time_hour_minute: str = "2200"  # The HHmm part for periodStart/End
    ):
        date_format = "%Y-%m-%d"
        try:
            current_date_obj = datetime.strptime(overall_start_date_str, date_format)
            end_date_loop_obj = datetime.strptime(overall_end_date_str, date_format)
        except ValueError:
            print("Error: Invalid date format. Please use yyyy-mm-dd.")
            return None

        all_daily_numpy_arrays = []
        total_points_processed = 0

        print(f"Fetching data for '{psr_name_to_extract}' from {overall_start_date_str} to {overall_end_date_str}")

        while current_date_obj <= end_date_loop_obj:
            period_start_formatted = current_date_obj.strftime("%Y%m%d") + time_hour_minute
            period_end_formatted = (current_date_obj + timedelta(days=1)).strftime("%Y%m%d") + time_hour_minute

            url = (f"{base_api_url}?documentType=A73&processType=A16"
                   f"&in_Domain={domain_eic}"
                   f"&periodStart={period_start_formatted}&periodEnd={period_end_formatted}"
                   f"&securityToken={self.api_key}")

            print(f"  Querying for date: {current_date_obj.strftime(date_format)}")  # Removed URL from this print

            max_api_retries = 10  # Max retries for 429 or network issues
            api_attempt = 0
            response_text_for_day = None  # Store successful response text here

            while api_attempt <= max_api_retries:
                # --- Rate Limiting Check (before each attempt) ---
                now = datetime.now()
                elapsed_since_window_start = (now - self.minute_window_start_time).total_seconds()

                if elapsed_since_window_start >= self.RATE_LIMIT_WINDOW_SECONDS:
                    print(
                        f"    Rate limit: Minute window expired. Resetting count from {self.request_count_this_minute}.")
                    self.request_count_this_minute = 0
                    self.minute_window_start_time = now
                    elapsed_since_window_start = 0

                if self.request_count_this_minute >= (self.MAX_REQUESTS_PER_MINUTE - self.RATE_LIMIT_BUFFER):
                    time_to_wait_for_next_window = self.RATE_LIMIT_WINDOW_SECONDS - elapsed_since_window_start
                    if time_to_wait_for_next_window > 0:
                        sleep_duration = time_to_wait_for_next_window + 1.0
                        print(
                            f"    Rate limit: Approaching limit ({self.request_count_this_minute} requests). Sleeping for {sleep_duration:.2f} seconds.")
                        time.sleep(sleep_duration)

                    self.request_count_this_minute = 0
                    self.minute_window_start_time = datetime.now()
                # --- End Rate Limiting Check ---

                try:
                    print(
                        f"    Attempting API request for {current_date_obj.strftime(date_format)} (attempt {api_attempt + 1})...")
                    response = requests.request("GET", url, headers={}, data={}, timeout=305)
                    self.request_count_this_minute += 1
                    print(
                        f"    Request count this minute: {self.request_count_this_minute} (Limit: {self.MAX_REQUESTS_PER_MINUTE - self.RATE_LIMIT_BUFFER})")

                    response.raise_for_status()
                    response_text_for_day = response.text
                    break  # API call successful, exit retry loop

                except requests.exceptions.HTTPError as http_err:
                    if http_err.response is not None and http_err.response.status_code == 429:
                        print(f"    RATE LIMIT HIT (429)! Banned for 10 minutes. Sleeping...")
                        time.sleep(60 * 10 + 5)  # Sleep for 10 minutes + 5 seconds
                        self.minute_window_start_time = datetime.now()  # Reset window after long sleep
                        self.request_count_this_minute = 0  # Reset count
                        api_attempt += 1
                        if api_attempt > max_api_retries:
                            print(
                                f"    Max retries for 429 reached for date {current_date_obj.strftime(date_format)}. Skipping this date.")
                        # Continue to next attempt or break if maxed out
                    else:
                        error_status = http_err.response.status_code if http_err.response is not None else "Unknown"
                        print(
                            f"    HTTP error ({error_status}) for {current_date_obj.strftime(date_format)}: {http_err}")
                        response_text_for_day = None
                        break  # Break from API retry loop for other HTTP errors
                except requests.exceptions.RequestException as req_err:  # Network errors
                    print(f"    Network error for {current_date_obj.strftime(date_format)}: {req_err}")
                    api_attempt += 1
                    if api_attempt > max_api_retries:
                        print(f"    Max retries for Network error reached. Skipping this date.")
                    else:
                        time.sleep(30)  # Wait before retrying network error

            # --- Process the response if successfully fetched ---
            if response_text_for_day:
                try:
                    if not response_text_for_day.strip():
                        print(f"    Warning: No data content returned for {current_date_obj.strftime(date_format)}")
                    else:
                        padded_xml = self.pad_hourly_to_15min(response_text_for_day)
                        if not padded_xml:
                            print(f"    Warning: Failed to pad XML for {current_date_obj.strftime(date_format)}")
                        else:
                            psr_specific_xml = self.extract_psr_data_to_xml(padded_xml, psr_name_to_extract)
                            if not psr_specific_xml:
                                # This is common if the PSR had no data on a particular day
                                pass
                            else:
                                daily_numpy_array = self.psr_xml_to_numpy(psr_specific_xml)
                                if daily_numpy_array is not None and daily_numpy_array.size > 0:
                                    # DO NOT UNCOMMENT THE NEXT LINE
                                    # daily_numpy_array[:, 0] += total_points_processed
                                    all_daily_numpy_arrays.append(daily_numpy_array)
                                    total_points_processed += daily_numpy_array.shape[0]
                                    print(
                                        f"    Successfully processed {daily_numpy_array.shape[0]} points for {current_date_obj.strftime(date_format)}.")
                                else:
                                    print(
                                        f"    Warning: No NumPy data extracted for {psr_name_to_extract} on {current_date_obj.strftime(date_format)}")
                except Exception as processing_err:
                    print(
                        f"    Error processing XML data for {current_date_obj.strftime(date_format)}: {processing_err}")
            else:
                print(
                    f"    No API response text to process for {current_date_obj.strftime(date_format)} after attempts.")

            # --- Increment date and politeness sleep ---
            current_date_obj += timedelta(days=1)
            if current_date_obj <= end_date_loop_obj:
                time.sleep(self.GENERAL_POLITENESS_SECONDS)

        if not all_daily_numpy_arrays:
            print(f"No data successfully processed for '{psr_name_to_extract}' in the given date range.")
            return None

        final_combined_array = np.concatenate(all_daily_numpy_arrays, axis=0)
        print(f"Successfully fetched and combined data. Total points: {final_combined_array.shape[0]}")
        return final_combined_array
    # </editor-fold>