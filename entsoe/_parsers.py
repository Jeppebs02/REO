import copy
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable

import numpy as np

_NAMESPACE_URI = 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
_NS = {'ns': _NAMESPACE_URI}

# Prevents ns0: prefixes in serialised output.
ET.register_namespace('', _NAMESPACE_URI)


def pad_hourly_to_15min(xml_string: str) -> str | None:
    """Convert PT60M TimeSeries in an ENTSO-E GL_MarketDocument to PT15M by repeating each point 4×."""
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error in pad_hourly_to_15min: {e}")
        return None

    for time_series in root.findall('.//ns:TimeSeries', _NS):
        for period in time_series.findall('.//ns:Period', _NS):
            resolution_el = period.find('ns:resolution', _NS)
            if resolution_el is None or resolution_el.text != 'PT60M':
                continue

            resolution_el.text = 'PT15M'
            original_points = list(period.findall('ns:Point', _NS))
            for pt in original_points:
                period.remove(pt)

            counter = 1
            for old_point in original_points:
                qty_el = old_point.find('ns:quantity', _NS)
                if qty_el is None:
                    continue
                qty_val = qty_el.text
                for _ in range(4):
                    new_pt = ET.Element(f"{{{_NAMESPACE_URI}}}Point")
                    pos = ET.SubElement(new_pt, f"{{{_NAMESPACE_URI}}}position")
                    pos.text = str(counter)
                    qty = ET.SubElement(new_pt, f"{{{_NAMESPACE_URI}}}quantity")
                    qty.text = qty_val
                    period.append(new_pt)
                    counter += 1

    return ET.tostring(root, encoding='unicode', xml_declaration=True)


def extract_psr_xml(xml_string: str, psr_name: str) -> str | None:
    """Return a new GL_MarketDocument XML string containing only the TimeSeries for psr_name."""
    try:
        original_root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error in extract_psr_xml: {e}")
        return None

    target_ts = None
    for ts in original_root.findall('.//ns:TimeSeries', _NS):
        name_el = ts.find('.//ns:MktPSRType/ns:PowerSystemResources/ns:name', _NS)
        if name_el is not None and name_el.text == psr_name:
            target_ts = ts
            break

    if target_ts is None:
        print(f"PSR with name '{psr_name}' not found in the XML.")
        return None

    new_root = ET.Element(f"{{{_NAMESPACE_URI}}}GL_MarketDocument")
    for key, value in original_root.attrib.items():
        new_root.set(key, value)

    header_tags = [
        'mRID', 'revisionNumber', 'type', 'process.processType',
        'sender_MarketParticipant.mRID', 'sender_MarketParticipant.marketRole.type',
        'receiver_MarketParticipant.mRID', 'receiver_MarketParticipant.marketRole.type',
        'createdDateTime', 'time_Period.timeInterval',
    ]
    for tag in header_tags:
        el = original_root.find(f'ns:{tag}', _NS)
        if el is not None:
            new_root.append(copy.deepcopy(el))

    new_root.append(copy.deepcopy(target_ts))
    return ET.tostring(new_root, encoding='unicode', xml_declaration=True)


def psr_xml_to_numpy(
        psr_xml_string: str,
        log_fn: Callable[[str, datetime, str], None] | None = None,
) -> np.ndarray | None:
    """Parse a single-PSR GL_MarketDocument XML string into a 2D object array [timestamp_str, quantity_float]."""
    psr_name = "UNKNOWN_PSR"
    date_obj = datetime.now()
    period_start_dt = None

    def _log(reason: str) -> None:
        if log_fn:
            log_fn(psr_name, date_obj, reason)

    try:
        root = ET.fromstring(psr_xml_string)
    except ET.ParseError as e:
        msg = f"XML Parsing Error in psr_xml_to_numpy: {e}"
        print(msg)
        _log(msg)
        return None

    name_el = root.find('.//ns:TimeSeries/ns:MktPSRType/ns:PowerSystemResources/ns:name', _NS)
    if name_el is not None and name_el.text:
        psr_name = name_el.text

    ts_el = root.find('.//ns:TimeSeries', _NS)
    if ts_el is None:
        msg = "No TimeSeries element found."
        print(f"Error for {psr_name}: {msg}")
        _log(msg)
        return None

    period_el = ts_el.find('.//ns:Period', _NS)
    if period_el is None:
        msg = "No Period element found within TimeSeries."
        print(f"Error for {psr_name}: {msg}")
        _log(msg)
        return None

    start_el = period_el.find('.//ns:timeInterval/ns:start', _NS)
    if start_el is None or not start_el.text:
        msg = "Could not find <Period><timeInterval><start>."
        print(f"Error for {psr_name}: {msg}")
        _log(msg)
        return None

    try:
        start_str = start_el.text
        if start_str.endswith('Z'):
            start_str = start_str[:-1] + "+00:00"
        period_start_dt = datetime.fromisoformat(start_str)
        date_obj = period_start_dt
    except ValueError as e:
        msg = f"Could not parse period start '{start_el.text}': {e}"
        print(f"Error for {psr_name}: {msg}")
        _log(msg)
        return None

    points_data = []
    fixed = 0
    total = 0

    for pt in period_el.findall('ns:Point', _NS):
        total += 1
        pos_el = pt.find('ns:position', _NS)
        qty_el = pt.find('ns:quantity', _NS)
        timestamp = "UNKNOWN_TIMESTAMP"
        pos_text = "N/A"

        if pos_el is not None and pos_el.text is not None:
            pos_text = pos_el.text
            try:
                pos = int(pos_el.text)
                hour_offset = (pos - 1) // 4
                timestamp = (period_start_dt + timedelta(hours=hour_offset)).strftime("%Y%m%d%H")
            except (ValueError, TypeError) as e:
                msg = (f"CRITICAL Point SKIPPED: Invalid position '{pos_el.text}' "
                       f"(orig qty: {qty_el.text if qty_el and qty_el.text else 'N/A'}). "
                       f"Cannot form timestamp. Error: {e}")
                print(f"Critical Warning for {psr_name}, {date_obj.strftime('%Y-%m-%d')}: {msg}")
                _log(msg)
                fixed += 1
                continue

        if qty_el is not None and qty_el.text is not None:
            try:
                points_data.append([timestamp, float(qty_el.text)])
            except (ValueError, TypeError) as e:
                msg = (f"FIXED Point: Timestamp={timestamp} (orig pos: {pos_text}, "
                       f"orig qty: {qty_el.text}) set to np.nan due to parsing error: {e}")
                print(f"Warning for {psr_name}, {date_obj.strftime('%Y-%m-%d')}: {msg}")
                _log(msg)
                points_data.append([timestamp, np.nan])
                fixed += 1
        elif pos_el is not None:
            msg = (f"FIXED Point: Timestamp={timestamp} (orig pos: {pos_text}) "
                   f"set to np.nan due to missing quantity.")
            print(f"Warning for {psr_name}, {date_obj.strftime('%Y-%m-%d')}: {msg}")
            _log(msg)
            points_data.append([timestamp, np.nan])
            fixed += 1

    if fixed > 0:
        _log(f"INFO: {fixed}/{total} point(s) had quantity set to np.nan or were skipped.")

    if not points_data:
        msg = "No <Point> elements found in XML Period." if total == 0 else "All points were skipped."
        print(f"Info for {psr_name}, {date_obj.strftime('%Y-%m-%d')}: {msg}")
        _log(msg)
        return None

    return np.array(points_data, dtype=object)


def parse_generation_by_type(xml_string: str, production_types: list[str]) -> np.ndarray | None:
    """Parse an A75 XML string into a float64 array of shape (96, len(production_types))."""
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error in parse_generation_by_type: {e}")
        return None

    data_by_type: dict[str, list[float]] = {}

    for ts in root.findall('.//ns:TimeSeries', _NS):
        psr_type_el = ts.find('.//ns:MktPSRType/ns:psrType', _NS)
        if psr_type_el is None or psr_type_el.text not in production_types:
            continue

        psr_type = psr_type_el.text
        resolution_el = ts.find('.//ns:Period/ns:resolution', _NS)
        if resolution_el is None or resolution_el.text != 'PT60M':
            print(f"Warning: Skipping TimeSeries for {psr_type} — "
                  f"unexpected resolution: {resolution_el.text if resolution_el is not None else 'N/A'}")
            continue

        points = ts.findall('.//ns:Period/ns:Point', _NS)
        point_map = {
            p.find('ns:position', _NS).text: p.find('ns:quantity', _NS).text
            for p in points
            if p.find('ns:position', _NS) is not None and p.find('ns:quantity', _NS) is not None
        }

        quantities: list[float] = []
        for i in range(1, 25):
            try:
                quantities.append(float(point_map.get(str(i), "-0.0001")))
            except (ValueError, TypeError):
                quantities.append(-0.0001)

        data_by_type[psr_type] = quantities

    daily = np.full((96, len(production_types)), -0.0001, dtype=float)
    for col, psr_type in enumerate(production_types):
        hourly = data_by_type.get(psr_type, [-0.0001] * 24)
        daily[:, col] = np.repeat(hourly, 4)

    return daily


def parse_physical_flow(xml_string: str) -> np.ndarray | None:
    """Parse an A11 XML string into a float64 array of shape (96,)."""
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"XML Parsing Error in parse_physical_flow: {e}")
        return None

    ts = root.find('.//{*}TimeSeries')
    if ts is None:
        print("Warning: No TimeSeries element found in physical flow XML.")
        return None

    resolution_el = ts.find('.//{*}Period/{*}resolution')
    if resolution_el is None:
        print("Warning: No resolution element found in physical flow XML.")
        return None

    points = ts.findall('.//{*}Period/{*}Point')
    point_map = {
        p.find('{*}position').text: p.find('{*}quantity').text
        for p in points
        if p.find('{*}position') is not None and p.find('{*}quantity') is not None
    }

    if resolution_el.text == 'PT15M':
        expected = 96
    elif resolution_el.text == 'PT60M':
        expected = 24
    else:
        print(f"Unsupported resolution in physical flow XML: {resolution_el.text}")
        return None

    quantities: list[float] = []
    for i in range(1, expected + 1):
        try:
            quantities.append(float(point_map.get(str(i), "-0.0001")))
        except (ValueError, TypeError):
            quantities.append(-0.0001)

    if resolution_el.text == 'PT60M':
        return np.repeat(quantities, 4)
    return np.array(quantities, dtype=float)


def placeholder_day(start_datetime: datetime, fill_value) -> np.ndarray:
    """Return a 96-row object array of [timestamp_str, fill_value] for a single day."""
    print(f"    Generating placeholder data for day starting "
          f"{start_datetime.strftime('%Y-%m-%d %H:%M')} with fill value '{fill_value}'.")
    rows = []
    for i in range(96):
        hour_dt = start_datetime + timedelta(hours=i // 4)
        rows.append([hour_dt.strftime("%Y%m%d%H"), fill_value])
    return np.array(rows, dtype=object)
