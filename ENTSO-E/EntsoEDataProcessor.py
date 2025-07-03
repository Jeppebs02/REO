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

    #Logging
    SKIPPED_DATES_LOG_FILE = "skipped_dates.log"


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


# <editor-fold desc="Helper functions">

    def log_skipped_date(self, psr_name: str, date_obj: datetime, reason: str):
        """Appends a skipped date entry to the log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} - SKIPPED: PSR='{psr_name}', Date='{date_obj.strftime('%Y-%m-%d')}', Reason='{reason}'\n"
        try:
            with open(self.SKIPPED_DATES_LOG_FILE, "a") as f:  # "a" for append mode
                f.write(log_entry)
            print(f"    Logged skipped date to {self.SKIPPED_DATES_LOG_FILE}")
        except Exception as e:
            print(f"    ERROR: Could not write to log file {self.SKIPPED_DATES_LOG_FILE}: {e}")

#</editor-fold>


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
    def psr_xml_to_numpy(self, psr_xml_string: str) -> np.ndarray | None:  # Removed extra logging params
        """
        Converts XML to NumPy. If point quantity parsing fails, sets quantity to np.nan and logs.
        Logs errors if the entire conversion fails or critical XML parts are missing.
        PSR name and date for logging are extracted from the XML string.

        Args:
            psr_xml_string: An XML string containing the data for a single PSR.
                            It's assumed this XML has one TimeSeries element.

        Returns:
            A 2D NumPy array or None if critical parsing fails or no valid points are found.
        """
        psr_name_for_logging = "UNKNOWN_PSR"  # Default if not found in XML
        date_obj_for_logging = datetime.now()  # Default to now, will be overwritten
        period_start_datetime_for_calc = None  # For calculating point timestamps

        try:
            root = ET.fromstring(psr_xml_string)
        except ET.ParseError as e:
            error_msg = f"XML Parsing Error in psr_xml_to_numpy: {e}"
            print(error_msg)
            # Cannot reliably get psr_name or date if XML parsing fails at root level
            self.log_skipped_date(psr_name_for_logging, date_obj_for_logging, error_msg)
            return None

        # Attempt to extract PSR Name for logging from the XML
        name_element_for_log = root.find('.//ns:TimeSeries/ns:MktPSRType/ns:PowerSystemResources/ns:name', self.NS)
        if name_element_for_log is not None and name_element_for_log.text:
            psr_name_for_logging = name_element_for_log.text
        else:
            print(f"Warning: Could not extract PSR name for logging from XML.")
            # Continue, but logging will use "UNKNOWN_PSR"

        time_series_element = root.find('.//ns:TimeSeries', self.NS)
        if time_series_element is None:
            error_msg = "No TimeSeries element found in the provided XML."
            print(f"Error for {psr_name_for_logging}: {error_msg}")
            self.log_skipped_date(psr_name_for_logging, date_obj_for_logging,
                                  error_msg)  # date_obj_for_logging might still be 'now'
            return None

        period_element = time_series_element.find('.//ns:Period', self.NS)
        if period_element is None:
            error_msg = "No Period element found within the TimeSeries."
            print(f"Error for {psr_name_for_logging}: {error_msg}")
            self.log_skipped_date(psr_name_for_logging, date_obj_for_logging, error_msg)
            return None

        period_start_time_str_el = period_element.find('.//ns:timeInterval/ns:start', self.NS)
        if period_start_time_str_el is None or not period_start_time_str_el.text:
            error_msg = "Could not find or parse <Period><timeInterval><start> from XML."
            print(f"Error for {psr_name_for_logging}: {error_msg}")
            self.log_skipped_date(psr_name_for_logging, date_obj_for_logging, error_msg)
            return None

        try:
            period_start_time_str = period_start_time_str_el.text
            if period_start_time_str.endswith('Z'):
                period_start_time_str = period_start_time_str[:-1] + "+00:00"
            # This datetime is crucial for point timestamp calculation AND for logging
            period_start_datetime_for_calc = datetime.fromisoformat(period_start_time_str)
            date_obj_for_logging = period_start_datetime_for_calc  # Use this for logging
        except ValueError as e:
            error_msg = f"Could not parse period start datetime string '{period_start_time_str_el.text}': {e}"
            print(f"Error for {psr_name_for_logging}: {error_msg}")
            self.log_skipped_date(psr_name_for_logging, date_obj_for_logging,
                                  error_msg)  # date_obj_for_logging might still be 'now'
            return None

        points_data = []
        fixed_points_count = 0
        total_points_in_xml = 0

        for point_element in period_element.findall('ns:Point', self.NS):
            total_points_in_xml += 1
            position_el = point_element.find('ns:position', self.NS)
            quantity_el = point_element.find('ns:quantity', self.NS)

            formatted_timestamp = "UNKNOWN_TIMESTAMP"
            point_position_15min_text = "N/A"

            if position_el is not None and position_el.text is not None:
                point_position_15min_text = position_el.text
                try:
                    point_position_15min = int(position_el.text)
                    hour_offset_from_period_start = (point_position_15min - 1) // 4
                    actual_hour_start_dt = period_start_datetime_for_calc + timedelta(
                        hours=hour_offset_from_period_start)
                    formatted_timestamp = actual_hour_start_dt.strftime("%Y%m%d%H")
                except (ValueError, TypeError) as e:
                    print(f"Critical Warning for {psr_name_for_logging}, {date_obj_for_logging.strftime('%Y-%m-%d')}: "
                          f"Could not parse point position: '{position_el.text}'. Error: {e}. Skipping this entire point.")
                    self.log_skipped_date(
                        psr_name_for_logging,
                        date_obj_for_logging,
                        f"CRITICAL Point SKIPPED: Invalid position '{position_el.text}' (orig qty: {quantity_el.text if quantity_el and quantity_el.text else 'N/A'}). Cannot form timestamp."
                    )
                    fixed_points_count += 1
                    continue

            if quantity_el is not None and quantity_el.text is not None:
                try:
                    quantity = float(quantity_el.text)
                    points_data.append([formatted_timestamp, quantity])
                except (ValueError, TypeError) as e:
                    print(
                        f"Warning for {psr_name_for_logging}, {date_obj_for_logging.strftime('%Y-%m-%d')} at timestamp {formatted_timestamp} (orig pos: {point_position_15min_text}): "
                        f"Could not parse quantity: '{quantity_el.text}'. Error: {e}. Setting quantity to np.nan.")
                    points_data.append([formatted_timestamp, np.nan])
                    fixed_points_count += 1
                    self.log_skipped_date(
                        psr_name_for_logging,
                        date_obj_for_logging,
                        f"FIXED Point: Timestamp={formatted_timestamp} (orig pos: {point_position_15min_text}, orig qty: {quantity_el.text}) set to np.nan due to parsing error: {e}"
                    )
            elif position_el is not None:  # Position valid, quantity missing
                print(
                    f"Warning for {psr_name_for_logging}, {date_obj_for_logging.strftime('%Y-%m-%d')} at timestamp {formatted_timestamp} (orig pos: {point_position_15min_text}): "
                    f"Quantity element missing or empty. Setting quantity to np.nan.")
                points_data.append([formatted_timestamp, np.nan])
                fixed_points_count += 1
                self.log_skipped_date(
                    psr_name_for_logging,
                    date_obj_for_logging,
                    f"FIXED Point: Timestamp={formatted_timestamp} (orig pos: {point_position_15min_text}) set to np.nan due to missing quantity."
                )
            # else: # Both position and quantity were problematic (handled by position check 'continue')

        if fixed_points_count > 0:
            self.log_skipped_date(
                psr_name_for_logging,
                date_obj_for_logging,
                f"INFO: {fixed_points_count}/{total_points_in_xml} point(s) had quantity set to np.nan or were skipped due to parsing/missing data."
            )

        if not points_data:
            if total_points_in_xml == 0:
                error_msg = "No <Point> elements found in XML Period."
                print(f"Info for {psr_name_for_logging}, {date_obj_for_logging.strftime('%Y-%m-%d')}: {error_msg}")
                self.log_skipped_date(psr_name_for_logging, date_obj_for_logging, error_msg)
            # If points_data is empty but total_points_in_xml > 0, it means all points were skipped
            # (e.g., all had bad positions), and this was already logged via fixed_points_count.
            return None

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
            date_skipped_for_psr = False

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
                    date_skipped_for_psr = False
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
                            self.log_skipped_date(psr_name_to_extract, current_date_obj, "Max retries for HTTP 429")
                            date_skipped_for_psr = True
                        # Continue to next attempt or break if maxed out
                    else:
                        error_status = http_err.response.status_code if http_err.response is not None else "Unknown"
                        print(
                            f"    HTTP error ({error_status}) for {current_date_obj.strftime(date_format)}: {http_err}")
                        response_text_for_day = None
                        self.log_skipped_date(psr_name_to_extract, current_date_obj, f"HTTP error {error_status}")
                        date_skipped_for_psr = True
                        break  # Break from API retry loop for other HTTP errors
                except requests.exceptions.RequestException as req_err:  # Network errors
                    print(f"    Network error for {current_date_obj.strftime(date_format)}: {req_err}")
                    api_attempt += 1
                    if api_attempt > max_api_retries:
                        print(f"    Max retries for Network error reached. Skipping this date.")
                        self.log_skipped_date(psr_name_to_extract, current_date_obj, f"Max retries for Network error ({type(req_err).__name__})")
                        date_skipped_for_psr = True
                    else:
                        time.sleep(30)  # Wait before retrying network error

            # --- Process the response if successfully fetched ---
            if not date_skipped_for_psr and response_text_for_day:
                try:
                    if not response_text_for_day.strip():
                        print(f"    Warning: No data content returned for {current_date_obj.strftime(date_format)}")
                        self.log_skipped_date(psr_name_to_extract, current_date_obj, "No data content returned")
                    else:
                        padded_xml = self.pad_hourly_to_15min(response_text_for_day)
                        if not padded_xml:
                            print(f"    Warning: Failed to pad XML for {current_date_obj.strftime(date_format)}")
                            self.log_skipped_date(psr_name_to_extract, current_date_obj, "Failed to pad XML")
                        else:
                            psr_specific_xml = self.extract_psr_data_to_xml(padded_xml, psr_name_to_extract)
                            if not psr_specific_xml:
                                # This is common if the PSR had no data on a particular day
                                self.log_skipped_date(psr_name_to_extract, current_date_obj, f"PSR '{psr_name_to_extract}' not found in daily XML")
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
                                    self.log_skipped_date(psr_name_to_extract, current_date_obj, "No NumPy data extracted from PSR-specific XML")
                except Exception as processing_err:
                    print(
                        f"    Error processing XML data for {current_date_obj.strftime(date_format)}: {processing_err}")
                    self.log_skipped_date(psr_name_to_extract, current_date_obj, f"Error during XML processing: {processing_err}")

            elif not date_skipped_for_psr and not response_text_for_day:
                print(
                    f"    No API response text to process for {current_date_obj.strftime(date_format)} after attempts.")
                self.log_skipped_date(psr_name_to_extract, current_date_obj, "No API response after all retries (unspecified reason)")

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



    # New functions for actual generation per production type.

    # <editor-fold desc="NEW - Parse Generation per Production Type">
    def parse_generation_per_type_to_numpy(self, xml_string: str,
                                           production_types_in_order: list[str]) -> np.ndarray | None:
        """
        Parses an A75 XML string (Actual Generation per Production Type) into a 2D NumPy array.
        The columns are ordered according to the 'production_types_in_order' list.

        Args:
            xml_string: The XML data string from the API.
            production_types_in_order: A list of psrType codes (e.g., ['B01', 'B05'])
                                       that defines the column order of the output array.

        Returns:
            A 2D NumPy array with 96 rows (for a 24-hour period padded to 15-min intervals)
            and columns for each production type, or None on critical failure.
        """
        try:
            root = ET.fromstring(xml_string)
        except ET.ParseError as e:
            print(f"XML Parsing Error in parse_generation_per_type_to_numpy: {e}")
            return None

        # Store the 24 hourly quantities for each found production type
        data_by_type = {}

        for time_series in root.findall('.//ns:TimeSeries', self.NS):
            psr_type_el = time_series.find('.//ns:MktPSRType/ns:psrType', self.NS)
            if psr_type_el is None or psr_type_el.text not in production_types_in_order:
                continue  # Skip TimeSeries that aren't for the types we want

            current_psr_type = psr_type_el.text

            # Check resolution. We only handle PT60M for now.
            resolution_el = time_series.find('.//ns:Period/ns:resolution', self.NS)
            is_hourly = resolution_el is not None and resolution_el.text == 'PT60M'
            if not is_hourly:
                print(
                    f"Warning: Skipping TimeSeries for {current_psr_type} due to non-hourly resolution: {resolution_el.text if resolution_el is not None else 'N/A'}")
                continue

            quantities = []
            points = time_series.findall('.//ns:Period/ns:Point', self.NS)
            # Create a dictionary for quick lookup of position -> quantity
            point_map = {p.find('ns:position', self.NS).text: p.find('ns:quantity', self.NS).text for p in points if
                         p.find('ns:position', self.NS) is not None and p.find('ns:quantity', self.NS) is not None}

            # Ensure we get 24 points, substituting 0 for any missing positions
            for i in range(1, 25):
                quantity_str = point_map.get(str(i), "0")  # Default to "0" if position is missing
                try:
                    quantities.append(float(quantity_str))
                except (ValueError, TypeError):
                    quantities.append(0.0)  # Use 0.0 if quantity is not a valid float

            data_by_type[current_psr_type] = quantities

        # Assemble the final daily array with padded data and correct column order
        num_columns = len(production_types_in_order)
        # Create an empty array: 96 rows (24 hours * 4 quarters), N columns
        daily_array = np.zeros((96, num_columns), dtype=float)

        for col_index, psr_type in enumerate(production_types_in_order):
            # Get the hourly data for this type, or a list of 24 zeros if not found in the XML
            hourly_data = data_by_type.get(psr_type, [0.0] * 24)

            # Pad to 15-minute intervals by repeating each value 4 times
            padded_data = np.repeat(hourly_data, 4)

            # Place the padded data into the correct column
            daily_array[:, col_index] = padded_data

        return daily_array

    # </editor-fold>

    # <editor-fold desc="NEW - Fetch Production by Type for Date Range">
    def fetch_production_by_type_for_range(
            self,
            overall_start_date_str: str,
            overall_end_date_str: str,
            domain_eic: str,
            production_types: list[str],  # The list defining the columns
            base_api_url: str = "https://web-api.tp.entsoe.eu/api"
    ):
        """
        Fetches 'Actual Generation per Production Type' (A75) for a date range
        with robust retry and logging logic.

        Args:
            overall_start_date_str: Start date "YYYY-MM-DD".
            overall_end_date_str: End date "YYYY-MM-DD".
            domain_eic: The EIC code for the bidding zone (e.g., "10YDK-1--------W").
            production_types: A list of psrType codes to extract, defining column order.

        Returns:
            A combined NumPy array for the entire period, or None if it fails.
        """
        date_format = "%Y-%m-%d"
        try:
            current_date_obj = datetime.strptime(overall_start_date_str, date_format)
            end_date_loop_obj = datetime.strptime(overall_end_date_str, date_format)
        except ValueError:
            print("Error: Invalid date format. Please use yyyy-mm-dd.")
            return None

        all_daily_arrays = []

        # Determine start/end time for API call. ENTSO-E days often run from 22:00 to 22:00 UTC
        time_hour_minute = "2200"

        print(f"Fetching Production by Type for {domain_eic} from {overall_start_date_str} to {overall_end_date_str}")

        while current_date_obj <= end_date_loop_obj:
            api_query_start_date = current_date_obj - timedelta(days=1)
            period_start_formatted = api_query_start_date.strftime("%Y%m%d") + time_hour_minute
            period_end_formatted = current_date_obj.strftime("%Y%m%d") + time_hour_minute

            url = (f"{base_api_url}?documentType=A75&processType=A16"  # documentType=A75
                   f"&in_Domain={domain_eic}"
                   f"&periodStart={period_start_formatted}&periodEnd={period_end_formatted}"
                   f"&securityToken={self.api_key}")

            print(f"  Querying for date: {current_date_obj.strftime(date_format)}")

            max_api_retries = 5  # Max retries for any single date
            api_attempt = 0
            response_text_for_day = None

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

                try:
                    print(
                        f"    Attempting API request for {current_date_obj.strftime(date_format)} (attempt {api_attempt + 1}/{max_api_retries + 1})...")
                    connect_timeout = 60
                    read_timeout = 120
                    response = requests.request("GET", url, headers={}, data={},
                                                timeout=(connect_timeout, read_timeout))

                    self.request_count_this_minute += 1
                    print(f"    Request count this minute: {self.request_count_this_minute}")

                    response.raise_for_status()
                    response_text_for_day = response.text
                    break

                except requests.exceptions.HTTPError as http_err:
                    reason = f"HTTPError: {http_err.response.status_code if http_err.response else 'Unknown'}"
                    if http_err.response is not None and http_err.response.status_code == 429:
                        print(f"    RATE LIMIT HIT (429)! Banned for 10 minutes. Sleeping...")
                        time.sleep(60 * 10 + 5)
                        self.minute_window_start_time = datetime.now()
                        self.request_count_this_minute = 0
                        api_attempt += 1
                        if api_attempt > max_api_retries:
                            print(
                                f"    Max retries for 429 reached for date {current_date_obj.strftime(date_format)}. Skipping.")
                            # Using a generic psr_name like "Production_Type_Data" for the log
                            self.log_skipped_date(f"Production_Type_Data-{domain_eic}", current_date_obj,
                                                  "Max retries for HTTP 429")
                    else:
                        print(f"    HTTP error ({reason}) for {current_date_obj.strftime(date_format)}: {http_err}")
                        self.log_skipped_date(f"Production_Type_Data-{domain_eic}", current_date_obj, reason)
                        break  # Break loop for other non-retryable HTTP errors
                except requests.exceptions.RequestException as req_err:
                    reason = f"RequestException: {type(req_err).__name__}"
                    print(
                        f"    Network error for {current_date_obj.strftime(date_format)} (attempt {api_attempt + 1}): {req_err}")
                    api_attempt += 1
                    if api_attempt > max_api_retries:
                        print(
                            f"    Max retries for Network error reached for date {current_date_obj.strftime(date_format)}. Skipping.")
                        self.log_skipped_date(f"Production_Type_Data-{domain_eic}", current_date_obj,
                                              f"Max retries for Network error ({type(req_err).__name__})")
                    else:
                        base_delay = 5
                        current_delay_exp = base_delay * (2 ** (api_attempt - 1))
                        jitter = np.random.uniform(0, 1)
                        sleep_duration = min(current_delay_exp + jitter, 120)
                        print(f"    Waiting {sleep_duration:.2f} seconds before retrying network error...")
                        time.sleep(sleep_duration)

            # --- Process the response if successfully fetched ---
            if response_text_for_day:
                daily_array = self.parse_generation_per_type_to_numpy(response_text_for_day, production_types)
                if daily_array is not None:
                    all_daily_arrays.append(daily_array)
                    print(f"    Successfully processed data for {current_date_obj.strftime(date_format)}.")
                else:
                    # Parsing failed, log this and append zeros to maintain structure
                    print(
                        f"    Warning: Parsing failed for {current_date_obj.strftime(date_format)}. Appending a block of zeros.")
                    self.log_skipped_date(f"Production_Type_Data-{domain_eic}", current_date_obj,
                                          "Failed to parse XML response")
                    all_daily_arrays.append(np.zeros((96, len(production_types))))
            else:
                # API fetch failed after all retries, log and append zeros
                print(
                    f"    API fetch failed for {current_date_obj.strftime(date_format)} after all retries. Appending a block of zeros.")
                # Logging is already handled inside the retry loop when max_retries is exceeded
                all_daily_arrays.append(np.zeros((96, len(production_types))))

            # --- Increment date and politeness sleep ---
            current_date_obj += timedelta(days=1)
            if current_date_obj <= end_date_loop_obj:
                time.sleep(self.GENERAL_POLITENESS_SECONDS)

        if not all_daily_arrays:
            print(f"No data successfully processed for {domain_eic} in the given date range.")
            return None

        return np.concatenate(all_daily_arrays, axis=0)

    # </editor-fold>

    # <editor-fold desc="NEW - Parse Physical Flow (A11)">
    def parse_physical_flow_to_numpy(self, xml_string: str) -> np.ndarray | None:
        """
        Parses an A11 XML string (Cross-Border Physical Flow) into a 1D NumPy array.
        It handles both PT15M and PT60M resolutions. If PT60M, it pads by repeating.

        Args:
            xml_string: The XML data string from the API.

        Returns:
            A 1D NumPy array with 96 values for a 24-hour period, or None on critical failure.
        """
        try:
            # The namespace for Publication_MarketDocument is slightly different.
            # Using a wildcard search for TimeSeries is often robust enough.
            root = ET.fromstring(xml_string)
        except ET.ParseError as e:
            print(f"XML Parsing Error in parse_physical_flow_to_numpy: {e}")
            return None

        # There should only be one TimeSeries for a physical flow query
        time_series = root.find('.//{*}TimeSeries')  # Use wildcard to ignore namespace
        if time_series is None:
            print("Warning: No TimeSeries element found in the physical flow XML.")
            return None  # Return None to indicate no data found in this file

        resolution_el = time_series.find('.//{*}Period/{*}resolution')
        if resolution_el is None:
            print("Warning: No resolution element found.")
            return None

        quantities = []
        points = time_series.findall('.//{*}Period/{*}Point')
        point_map = {p.find('{*}position').text: p.find('{*}quantity').text for p in points if
                     p.find('{*}position') is not None and p.find('{*}quantity') is not None}

        num_expected_points = 0
        if resolution_el.text == 'PT15M':
            num_expected_points = 96
        elif resolution_el.text == 'PT60M':
            num_expected_points = 24
        else:
            print(f"Unsupported resolution found: {resolution_el.text}")
            return None

        for i in range(1, num_expected_points + 1):
            quantity_str = point_map.get(str(i), "0")
            try:
                quantities.append(float(quantity_str))
            except (ValueError, TypeError):
                quantities.append(0.0)

        # If data was hourly, pad it. Otherwise, it's already 96 points.
        if resolution_el.text == 'PT60M':
            padded_quantities = np.repeat(quantities, 4)
            return padded_quantities
        else:  # PT15M
            return np.array(quantities, dtype=float)

    # </editor-fold>

    # <editor-fold desc="NEW - Fetch Physical Flow for Date Range">
    def fetch_physical_flow_for_range(
            self,
            overall_start_date_str: str,
            overall_end_date_str: str,
            in_domain_eic: str,
            out_domain_eic: str,
            base_api_url: str = "https://web-api.tp.entsoe.eu/api"
    ):
        """
        Fetches Cross-Border Physical Flow (A11) for a date range and a specific direction,
        with robust retry and logging logic.

        Args:
            overall_start_date_str: Start date "YYYY-MM-DD".
            overall_end_date_str: End date "YYYY-MM-DD".
            in_domain_eic: The EIC code of the 'from' area.
            out_domain_eic: The EIC code of the 'to' area.

        Returns:
            A combined 1D NumPy array for the entire period, or None if it fails.
        """
        date_format = "%Y-%m-%d"
        try:
            current_date_obj = datetime.strptime(overall_start_date_str, date_format)
            end_date_loop_obj = datetime.strptime(overall_end_date_str, date_format)
        except ValueError:
            print("Error: Invalid date format. Please use yyyy-mm-dd.")
            return None

        all_daily_arrays = []
        time_hour_minute = "2200"

        log_psr_name = f"Physical_Flow-{in_domain_eic}-to-{out_domain_eic}"
        print(f"Fetching {log_psr_name} from {overall_start_date_str} to {overall_end_date_str}")

        while current_date_obj <= end_date_loop_obj:
            api_query_start_date = current_date_obj - timedelta(days=1)
            period_start_formatted = api_query_start_date.strftime("%Y%m%d") + time_hour_minute
            period_end_formatted = current_date_obj.strftime("%Y%m%d") + time_hour_minute

            url = (f"{base_api_url}?documentType=A11&processType=A16"
                   f"&in_Domain={in_domain_eic}"
                   f"&out_Domain={out_domain_eic}"
                   f"&periodStart={period_start_formatted}&periodEnd={period_end_formatted}"
                   f"&securityToken={self.api_key}")

            print(f"  Querying for date: {current_date_obj.strftime(date_format)}")

            max_api_retries = 5
            api_attempt = 0
            response_text_for_day = None

            while api_attempt <= max_api_retries:
                # --- Rate Limiting Check ---
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

                try:
                    print(
                        f"    Attempting API request for {current_date_obj.strftime(date_format)} (attempt {api_attempt + 1}/{max_api_retries + 1})...")
                    connect_timeout = 60
                    read_timeout = 120
                    response = requests.request("GET", url, headers={}, data={},
                                                timeout=(connect_timeout, read_timeout))

                    self.request_count_this_minute += 1
                    print(f"    Request count this minute: {self.request_count_this_minute}")

                    response.raise_for_status()
                    response_text_for_day = response.text
                    break

                except requests.exceptions.HTTPError as http_err:
                    reason = f"HTTPError: {http_err.response.status_code if http_err.response else 'Unknown'}"
                    if http_err.response is not None and http_err.response.status_code == 429:
                        print(f"    RATE LIMIT HIT (429)! Banned for 10 minutes. Sleeping...")
                        time.sleep(60 * 10 + 5)
                        self.minute_window_start_time = datetime.now()
                        self.request_count_this_minute = 0
                        api_attempt += 1
                        if api_attempt > max_api_retries:
                            print(
                                f"    Max retries for 429 reached for date {current_date_obj.strftime(date_format)}. Skipping.")
                            self.log_skipped_date(log_psr_name, current_date_obj, "Max retries for HTTP 429")
                    else:
                        print(f"    HTTP error ({reason}) for {current_date_obj.strftime(date_format)}: {http_err}")
                        self.log_skipped_date(log_psr_name, current_date_obj, reason)
                        break
                except requests.exceptions.RequestException as req_err:
                    reason = f"RequestException: {type(req_err).__name__}"
                    print(
                        f"    Network error for {current_date_obj.strftime(date_format)} (attempt {api_attempt + 1}): {req_err}")
                    api_attempt += 1
                    if api_attempt > max_api_retries:
                        print(
                            f"    Max retries for Network error reached for date {current_date_obj.strftime(date_format)}. Skipping.")
                        self.log_skipped_date(log_psr_name, current_date_obj,
                                              f"Max retries for Network error ({type(req_err).__name__})")
                    else:
                        base_delay = 5
                        current_delay_exp = base_delay * (2 ** (api_attempt - 1))
                        jitter = np.random.uniform(0, 1)
                        sleep_duration = min(current_delay_exp + jitter, 120)
                        print(f"    Waiting {sleep_duration:.2f} seconds before retrying network error...")
                        time.sleep(sleep_duration)

            # --- Process the response if successfully fetched ---
            if response_text_for_day:
                daily_array = self.parse_physical_flow_to_numpy(response_text_for_day)
                if daily_array is not None:
                    all_daily_arrays.append(daily_array)
                    print(f"    Successfully processed data for {current_date_obj.strftime(date_format)}.")
                else:
                    print(
                        f"    Warning: Parsing returned no data for {current_date_obj.strftime(date_format)}. Appending a block of zeros.")
                    self.log_skipped_date(log_psr_name, current_date_obj,
                                          "XML response parsed, but resulted in no data (e.g., no TimeSeries).")
                    all_daily_arrays.append(np.zeros(96))
            else:
                # API fetch failed after all retries, log and append zeros
                print(
                    f"    API fetch failed for {current_date_obj.strftime(date_format)} after all retries. Appending a block of zeros.")
                # Logging is already handled inside the retry loop when max_retries is exceeded
                all_daily_arrays.append(np.zeros(96))

            # --- Increment date and politeness sleep ---
            current_date_obj += timedelta(days=1)
            if current_date_obj <= end_date_loop_obj:
                time.sleep(self.GENERAL_POLITENESS_SECONDS)

        if not all_daily_arrays:
            print(f"No data successfully processed for {log_psr_name} in the given date range.")
            return None

        return np.concatenate(all_daily_arrays, axis=0)
    # </editor-fold>