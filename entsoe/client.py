import time
from datetime import datetime, timedelta

import numpy as np

from entsoe._http import HttpClient
from entsoe._parsers import (
    extract_psr_xml,
    pad_hourly_to_15min,
    parse_generation_by_type,
    parse_physical_flow,
    placeholder_day,
    psr_xml_to_numpy,
)

_BASE_API_URL = "https://web-api.tp.entsoe.eu/api"
_DATE_FMT = "%Y-%m-%d"

_ZONE_EIC = {
    "DK1": "10Y1001A1001A796",
    "DK2": "10YDK-2--------M",
    "DE":  "10Y1001A1001A83F",
}


def _resolve_eic(zone_or_eic: str) -> str:
    return _ZONE_EIC.get(zone_or_eic, zone_or_eic)


class EntsoEClient:

    def __init__(self, api_key: str, skipped_log: str = "skipped_dates.log"):
        self._http = HttpClient(api_key, skipped_log)

    # ── Primary methods ───────────────────────────────────────────────────────

    def actual_generation_per_unit(
            self,
            zone: str,
            psr_name: str,
            start: str,
            end: str,
            registered_resource: str | None = None,
            pad_missing_days: bool = False,
            fill_value=np.nan,
            time_hour_minute: str = "2200",
            base_api_url: str = _BASE_API_URL,
    ) -> np.ndarray | None:
        """Return 15-min actual generation for a single unit (A73, doc 16.1.A)."""
        domain_eic = _resolve_eic(zone)
        try:
            current = datetime.strptime(start, _DATE_FMT)
            end_dt = datetime.strptime(end, _DATE_FMT)
        except ValueError:
            print("Error: Invalid date format. Please use YYYY-MM-DD.")
            return None

        print(f"Fetching data for '{psr_name}' from {start} to {end}")
        resource_param = f"&RegisteredResource={registered_resource}" if registered_resource else ""
        daily_arrays: list[np.ndarray] = []

        while current <= end_dt:
            start_dt = datetime.strptime(current.strftime("%Y%m%d") + time_hour_minute, "%Y%m%d%H%M")
            end_period = start_dt + timedelta(days=1)
            url = (
                f"{base_api_url}?documentType=A73&processType=A16"
                f"&in_Domain={domain_eic}"
                f"{resource_param}"
                f"&periodStart={start_dt.strftime('%Y%m%d%H%M')}"
                f"&periodEnd={end_period.strftime('%Y%m%d%H%M')}"
                f"&securityToken={self._http.api_key}"
            )
            print(f"  Querying for period starting: {start_dt.strftime('%Y-%m-%d %H:%M')}")

            xml = self._http.get(url, psr_name, current)

            if xml and "<Reason>" in xml and "No matching data found" in xml:
                print("    API returned 'No matching data found'.")
                self._http._log_skipped(psr_name, current, "API returned 'No matching data found'")
                xml = None

            if xml:
                try:
                    padded = pad_hourly_to_15min(xml)
                    if padded:
                        psr_xml = extract_psr_xml(padded, psr_name)
                        if psr_xml:
                            day_array = psr_xml_to_numpy(psr_xml, log_fn=self._http._log_skipped)
                            if day_array is not None and day_array.size > 0:
                                daily_arrays.append(day_array)
                                print(f"    Successfully processed {day_array.shape[0]} points.")
                            elif pad_missing_days:
                                daily_arrays.append(placeholder_day(start_dt, fill_value))
                        elif pad_missing_days:
                            daily_arrays.append(placeholder_day(start_dt, fill_value))
                    elif pad_missing_days:
                        daily_arrays.append(placeholder_day(start_dt, fill_value))
                except Exception as e:
                    print(f"    Unexpected error during processing: {e}")
                    if pad_missing_days:
                        daily_arrays.append(placeholder_day(start_dt, fill_value))
            else:
                print(f"    No valid data document for {current.strftime(_DATE_FMT)}.")
                if pad_missing_days:
                    daily_arrays.append(placeholder_day(start_dt, fill_value))

            current += timedelta(days=1)
            if current <= end_dt:
                time.sleep(self._http.POLITENESS_SLEEP)

        if not daily_arrays:
            print(f"No data processed or padded for '{psr_name}' in the given range.")
            return None
        return np.concatenate(daily_arrays, axis=0)

    def actual_generation_per_type(
            self,
            zone: str,
            production_types: list[str],
            start: str,
            end: str,
            base_api_url: str = _BASE_API_URL,
    ) -> np.ndarray | None:
        """Return 15-min actual generation per production type for a zone (A75, doc 16.1.B&C)."""
        domain_eic = _resolve_eic(zone)
        log_context = f"Production_Type_Data-{domain_eic}"
        try:
            current = datetime.strptime(start, _DATE_FMT)
            end_dt = datetime.strptime(end, _DATE_FMT)
        except ValueError:
            print("Error: Invalid date format. Please use YYYY-MM-DD.")
            return None

        print(f"Fetching Production by Type for {domain_eic} from {start} to {end}")
        daily_arrays: list[np.ndarray] = []

        while current <= end_dt:
            # ENTSO-E A75 days run 22:00–22:00 UTC
            api_start = (current - timedelta(days=1)).strftime("%Y%m%d") + "2200"
            api_end = current.strftime("%Y%m%d") + "2200"
            url = (
                f"{base_api_url}?documentType=A75&processType=A16"
                f"&in_Domain={domain_eic}"
                f"&periodStart={api_start}&periodEnd={api_end}"
                f"&securityToken={self._http.api_key}"
            )
            print(f"  Querying for date: {current.strftime(_DATE_FMT)}")

            xml = self._http.get(url, log_context, current)

            if xml:
                day_array = parse_generation_by_type(xml, production_types)
                if day_array is not None:
                    daily_arrays.append(day_array)
                    print(f"    Successfully processed data for {current.strftime(_DATE_FMT)}.")
                else:
                    print(f"    Parsing failed for {current.strftime(_DATE_FMT)}. Appending zeros.")
                    self._http._log_skipped(log_context, current, "Failed to parse XML response")
                    daily_arrays.append(np.zeros((96, len(production_types))))
            else:
                print(f"    API fetch failed for {current.strftime(_DATE_FMT)}. Appending zeros.")
                daily_arrays.append(np.zeros((96, len(production_types))))

            current += timedelta(days=1)
            if current <= end_dt:
                time.sleep(self._http.POLITENESS_SLEEP)

        if not daily_arrays:
            print(f"No data processed for {domain_eic} in the given range.")
            return None
        return np.concatenate(daily_arrays, axis=0)

    def physical_flow(
            self,
            from_zone: str,
            to_zone: str,
            start: str,
            end: str,
            base_api_url: str = _BASE_API_URL,
    ) -> np.ndarray | None:
        """Return 15-min cross-border physical flow between two zones (A11, doc 12.1.G)."""
        in_eic = _resolve_eic(from_zone)
        out_eic = _resolve_eic(to_zone)
        log_context = f"Physical_Flow-{in_eic}-to-{out_eic}"
        try:
            current = datetime.strptime(start, _DATE_FMT)
            end_dt = datetime.strptime(end, _DATE_FMT)
        except ValueError:
            print("Error: Invalid date format. Please use YYYY-MM-DD.")
            return None

        print(f"Fetching {log_context} from {start} to {end}")
        daily_arrays: list[np.ndarray] = []

        while current <= end_dt:
            # ENTSO-E A11 days run 22:00–22:00 UTC
            api_start = (current - timedelta(days=1)).strftime("%Y%m%d") + "2200"
            api_end = current.strftime("%Y%m%d") + "2200"
            url = (
                f"{base_api_url}?documentType=A11&processType=A16"
                f"&in_Domain={in_eic}&out_Domain={out_eic}"
                f"&periodStart={api_start}&periodEnd={api_end}"
                f"&securityToken={self._http.api_key}"
            )
            print(f"  Querying for date: {current.strftime(_DATE_FMT)}")

            xml = self._http.get(url, log_context, current)

            if xml:
                day_array = parse_physical_flow(xml)
                if day_array is not None:
                    daily_arrays.append(day_array)
                    print(f"    Successfully processed data for {current.strftime(_DATE_FMT)}.")
                else:
                    print(f"    Parsing returned no data for {current.strftime(_DATE_FMT)}. Appending zeros.")
                    self._http._log_skipped(
                        log_context, current,
                        "XML response parsed but resulted in no data (e.g. no TimeSeries).",
                    )
                    daily_arrays.append(np.zeros(96))
            else:
                print(f"    API fetch failed for {current.strftime(_DATE_FMT)}. Appending zeros.")
                daily_arrays.append(np.zeros(96))

            current += timedelta(days=1)
            if current <= end_dt:
                time.sleep(self._http.POLITENESS_SLEEP)

        if not daily_arrays:
            print(f"No data processed for {log_context} in the given range.")
            return None
        return np.concatenate(daily_arrays, axis=0)

    # ── Stubs for documented endpoints not yet implemented ────────────────────

    def installed_capacity_per_type(self, zone: str, start: str, end: str) -> np.ndarray | None:
        """Return installed generation capacity per type (A68, doc 14.1.A)."""
        raise NotImplementedError("installed_capacity_per_type not implemented (doc 14.1.A, A68)")

    def installed_capacity_per_unit(self, zone: str, start: str, end: str) -> np.ndarray | None:
        """Return installed generation capacity per unit (A71, doc 14.1.B)."""
        raise NotImplementedError("installed_capacity_per_unit not implemented (doc 14.1.B, A71)")

    def day_ahead_generation_forecast(self, zone: str, start: str, end: str) -> np.ndarray | None:
        """Return day-ahead generation forecast (A69, doc 14.1.C)."""
        raise NotImplementedError("day_ahead_generation_forecast not implemented (doc 14.1.C, A69)")

    def wind_solar_forecast(self, zone: str, start: str, end: str) -> np.ndarray | None:
        """Return wind and solar day-ahead forecast (A69, doc 14.1.D)."""
        raise NotImplementedError("wind_solar_forecast not implemented (doc 14.1.D, A69)")

    def forecasted_transfer_capacity(
            self, from_zone: str, to_zone: str, start: str, end: str,
    ) -> np.ndarray | None:
        """Return forecasted transfer capacity between zones (A61, doc 11.1.A)."""
        raise NotImplementedError("forecasted_transfer_capacity not implemented (doc 11.1.A, A61)")

    def actual_total_load(self, zone: str, start: str, end: str) -> np.ndarray | None:
        """Return actual total load for a zone (A65, doc 6.1.A)."""
        raise NotImplementedError("actual_total_load not implemented (doc 6.1.A, A65)")

    def day_ahead_load_forecast(self, zone: str, start: str, end: str) -> np.ndarray | None:
        """Return day-ahead total load forecast for a zone (A65, doc 6.1.B)."""
        raise NotImplementedError("day_ahead_load_forecast not implemented (doc 6.1.B, A65)")

    # ── Backward-compat wrappers (removed in Phase 2) ────────────────────────

    def fetch_and_process_psr_data_range_new(
            self,
            overall_start_date_str: str,
            overall_end_date_str: str,
            domain_eic: str,
            psr_name_to_extract: str,
            base_api_url: str = _BASE_API_URL,
            time_hour_minute: str = "2200",
            pad_missing_days: bool = False,
            fill_value=np.nan,
            registered_resource: str | None = None,
    ) -> np.ndarray | None:
        """Backward-compat wrapper → actual_generation_per_unit (A73, doc 16.1.A)."""
        return self.actual_generation_per_unit(
            zone=domain_eic,
            psr_name=psr_name_to_extract,
            start=overall_start_date_str,
            end=overall_end_date_str,
            registered_resource=registered_resource,
            pad_missing_days=pad_missing_days,
            fill_value=fill_value,
            time_hour_minute=time_hour_minute,
            base_api_url=base_api_url,
        )

    def fetch_and_process_psr_data_range(
            self,
            overall_start_date_str: str,
            overall_end_date_str: str,
            domain_eic: str,
            psr_name_to_extract: str,
            base_api_url: str = _BASE_API_URL,
            time_hour_minute: str = "2200",
            pad_missing_days: bool = False,
            fill_value=np.nan,
    ) -> np.ndarray | None:
        """Backward-compat wrapper → actual_generation_per_unit (A73, doc 16.1.A)."""
        return self.actual_generation_per_unit(
            zone=domain_eic,
            psr_name=psr_name_to_extract,
            start=overall_start_date_str,
            end=overall_end_date_str,
            pad_missing_days=pad_missing_days,
            fill_value=fill_value,
            time_hour_minute=time_hour_minute,
            base_api_url=base_api_url,
        )

    def fetch_production_by_type_for_range(
            self,
            overall_start_date_str: str,
            overall_end_date_str: str,
            domain_eic: str,
            production_types: list[str],
            base_api_url: str = _BASE_API_URL,
    ) -> np.ndarray | None:
        """Backward-compat wrapper → actual_generation_per_type (A75, doc 16.1.B&C)."""
        return self.actual_generation_per_type(
            zone=domain_eic,
            production_types=production_types,
            start=overall_start_date_str,
            end=overall_end_date_str,
            base_api_url=base_api_url,
        )

    def fetch_physical_flow_for_range(
            self,
            overall_start_date_str: str,
            overall_end_date_str: str,
            in_domain_eic: str,
            out_domain_eic: str,
            base_api_url: str = _BASE_API_URL,
    ) -> np.ndarray | None:
        """Backward-compat wrapper → physical_flow (A11, doc 12.1.G)."""
        return self.physical_flow(
            from_zone=in_domain_eic,
            to_zone=out_domain_eic,
            start=overall_start_date_str,
            end=overall_end_date_str,
            base_api_url=base_api_url,
        )
