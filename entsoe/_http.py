import time
import numpy as np
import requests
from datetime import datetime


class HttpClient:
    MAX_REQUESTS_PER_MINUTE = 400
    _RATE_LIMIT_BUFFER = 10
    _RATE_LIMIT_WINDOW = 60.0
    POLITENESS_SLEEP = 0.5

    def __init__(self, api_key: str, skipped_log: str = "skipped_dates.log"):
        self.api_key = api_key
        self._skipped_log = skipped_log
        self._request_count = 0
        self._window_start = datetime.now()

    def get(self, url: str, log_context: str, date_obj: datetime) -> str | None:
        """GET with rate limiting, retry on 429/network errors, skipped_dates.log on failure."""
        max_retries = 5
        attempt = 0

        while attempt <= max_retries:
            self._check_rate_limit()

            try:
                print(f"    Attempting API request for {date_obj.strftime('%Y-%m-%d')} "
                      f"(attempt {attempt + 1}/{max_retries + 1})...")
                response = requests.request("GET", url, headers={}, data={}, timeout=(60, 120))
                self._request_count += 1
                print(f"    Request count this minute: {self._request_count}")
                response.raise_for_status()
                # Explicit UTF-8 decode: requests defaults to ISO-8859-1 for text/xml which
                # garbles Danish characters (e.g. å → Ã¥).
                return response.content.decode('utf-8')

            except requests.exceptions.HTTPError as http_err:
                if http_err.response is not None and http_err.response.status_code == 429:
                    print("    RATE LIMIT HIT (429)! Banned for 10 minutes. Sleeping...")
                    time.sleep(60 * 10 + 5)
                    self._window_start = datetime.now()
                    self._request_count = 0
                    attempt += 1
                else:
                    status = (http_err.response.status_code
                              if http_err.response is not None else "Unknown")
                    print(f"    HTTP error ({status}) for {date_obj.strftime('%Y-%m-%d')}: {http_err}")
                    self._log_skipped(log_context, date_obj, f"HTTP error {status}")
                    return None

            except requests.exceptions.RequestException as req_err:
                print(f"    Network error for {date_obj.strftime('%Y-%m-%d')} "
                      f"(attempt {attempt + 1}): {req_err}")
                attempt += 1
                if attempt > max_retries:
                    break
                delay = min(5 * (2 ** (attempt - 1)) + np.random.uniform(0, 1), 120)
                print(f"    Waiting {delay:.2f} seconds before retrying...")
                time.sleep(delay)

        print(f"    Max retries reached for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
        self._log_skipped(log_context, date_obj, "Max retries reached for API request")
        return None

    def _check_rate_limit(self) -> None:
        now = datetime.now()
        elapsed = (now - self._window_start).total_seconds()

        if elapsed >= self._RATE_LIMIT_WINDOW:
            print(f"    Rate limit: Minute window expired. Resetting count from {self._request_count}.")
            self._request_count = 0
            self._window_start = now
            return

        if self._request_count >= (self.MAX_REQUESTS_PER_MINUTE - self._RATE_LIMIT_BUFFER):
            wait = self._RATE_LIMIT_WINDOW - elapsed + 1.0
            print(f"    Rate limit: Approaching limit ({self._request_count} requests). "
                  f"Sleeping for {wait:.2f} seconds.")
            time.sleep(wait)
            self._request_count = 0
            self._window_start = datetime.now()

    def _log_skipped(self, context: str, date_obj: datetime, reason: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = (f"{timestamp} - SKIPPED: PSR='{context}', "
                 f"Date='{date_obj.strftime('%Y-%m-%d')}', Reason='{reason}'\n")
        try:
            with open(self._skipped_log, "a") as f:
                f.write(entry)
            print(f"    Logged skipped date to {self._skipped_log}")
        except Exception as e:
            print(f"    ERROR: Could not write to log file {self._skipped_log}: {e}")
