# Plan: ENTSO-E Client Refactor

## Goal

Replace the monolithic `EntsoEDataProcessor.py` with a small package that:

1. Has **one public class** — `EntsoEClient` — that callers import and ask for data.
2. Has **one method per documented endpoint** (mapped 1:1 to the doc pages in `docs/ENTSO-E/`).
3. Keeps internal complexity hidden in private modules that are never imported directly.

---

## Proposed structure

```
ENTSO-E/
  entsoe/                      ← new package folder
    __init__.py                ← exports only EntsoEClient
    client.py                  ← EntsoEClient — the single public class
    _http.py                   ← HttpClient: rate limiting, retry, _perform_robust_request
    _parsers.py                ← pure XML→numpy functions (no state, no network)
  EntsoEDataProcessor.py       ← kept as-is until refactor is complete, then removed
  data_importer/               ← unchanged
  ...
```

Callers only ever write:

```python
from entsoe import EntsoEClient
client = EntsoEClient(api_key="...")
data = client.actual_generation_per_unit(zone="DK1", psr_name="Solar Park Kassoe",
                                         start="2025-01-01", end="2026-06-12")
```

---

## Layer breakdown

### `_http.py` — `HttpClient`

Everything that touches the network in one place:

- Rate limit state (request counter, window timer)
- `get(url) -> str | None` — the only public method: GET with retries, 429 backoff, exponential backoff for network errors, `skipped_dates.log` writes
- No XML, no numpy

### `_parsers.py` — pure functions

Stateless XML→numpy conversions. No class, no network.

| Function | Source in old file |
|---|---|
| `pad_hourly_to_15min(xml) -> str` | `pad_hourly_to_15min` |
| `extract_psr_xml(xml, psr_name) -> str \| None` | `extract_psr_data_to_xml` |
| `psr_xml_to_numpy(xml) -> ndarray \| None` | `psr_xml_to_numpy` |
| `parse_generation_by_type(xml, types) -> ndarray \| None` | `parse_generation_per_type_to_numpy` |
| `parse_physical_flow(xml) -> ndarray \| None` | `parse_physical_flow_to_numpy` |
| `placeholder_day(start_dt, fill) -> ndarray` | `_generate_placeholder_day` |

### `client.py` — `EntsoEClient`

One method per endpoint. Internally uses `HttpClient` + parser functions.
All methods share the same signature shape:
- `start` / `end` as `"YYYY-MM-DD"` strings
- `zone` as `"DK1"` / `"DK2"` (client maps to EIC internally)
- Returns a numpy array or `None`

---

## Endpoint methods (mapped to docs)

### Generation (`docs/ENTSO-E/Generation/`)

| Method | Doc page | Document type |
|---|---|---|
| `actual_generation_per_unit(zone, psr_name, start, end)` | 16.1.A | A73 |
| `actual_generation_per_type(zone, production_types, start, end)` | 16.1.B&C | A75 |
| `installed_capacity_per_type(zone, start, end)` | 14.1.A | A68 |
| `installed_capacity_per_unit(zone, start, end)` | 14.1.B | A71 |
| `day_ahead_generation_forecast(zone, start, end)` | 14.1.C | A69 |
| `wind_solar_forecast(zone, start, end)` | 14.1.D | A69 |

### Transmission (`docs/ENTSO-E/Transmission/`)

| Method | Doc page | Document type |
|---|---|---|
| `physical_flow(from_zone, to_zone, start, end)` | 12.1.G | A11 |
| `forecasted_transfer_capacity(from_zone, to_zone, start, end)` | 11.1.A | A61 |

### Load (`docs/ENTSO-E/Load/`)

| Method | Doc page | Document type |
|---|---|---|
| `actual_total_load(zone, start, end)` | 6.1.A | A65 |
| `day_ahead_load_forecast(zone, start, end)` | 6.1.B | A65 |

> **Note:** Methods beyond what's currently implemented are stubs that raise `NotImplementedError`. They establish the interface without forcing implementation of every endpoint upfront.

---

## Zone EIC map (lives in `client.py`)

```python
_ZONE_EIC = {
    "DK1": "10Y1001A1001A796",
    "DK2": "10YDK-2--------M",
    "DE":  "10Y1001A1001A83F",
    # add more as needed
}
```

---

## Migration path (phased)

### Phase 1 — Create the package skeleton (no behaviour changes)
- Create `ENTSO-E/entsoe/__init__.py`, `_http.py`, `_parsers.py`, `client.py`
- Move code out of `EntsoEDataProcessor.py` method-by-method, **no logic changes**
- `EntsoEDataProcessor.py` temporarily re-exports `EntsoEClient` as `EntsoeDataProcessor` for backward compatibility
- Verify: all existing scripts (`solar_4farms_2025_2026.py`, `dk_solar_wind_2.py`, `data_importer/importer.py`) still work unchanged

### Phase 2 — Update callers
- Update the three scripts above to `from entsoe import EntsoEClient`
- Remove the backward-compat shim from `EntsoEDataProcessor.py`
- Delete `EntsoEDataProcessor.py`

### Phase 3 — Add stub methods
- Add `NotImplementedError` stubs for all documented endpoints not yet implemented
- No new fetching logic — just establishes the public API surface

---

## What stays the same

- All numpy array shapes and dtypes are unchanged
- `pad_hourly_to_15min` logic is unchanged
- `time_hour_minute="0000"` convention for DK solar/wind stays
- `skipped_dates.log` behaviour stays
- Rate limit constants stay

---

## Open questions (need your input before starting)

1. **Where should the package live?** `ENTSO-E/entsoe/` keeps it inside the existing folder. Alternatively `entsoe/` at the repo root would make it importable from anywhere without `sys.path` tricks. Which do you prefer?

2. **Stubs vs. skip?** Add `NotImplementedError` stubs for undocumented endpoints now (establishes the full surface), or only implement what we actually use (leaner)?

3. **`data_importer/importer.py`** currently uses `sys.path` to reach `EntsoEDataProcessor`. After the refactor, if the package is at `ENTSO-E/entsoe/`, `importer.py` can do a clean `from entsoe import EntsoEClient`. Worth doing in Phase 2, or keep it as a separate task?
