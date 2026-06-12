# Plan: ENTSO-E Client Refactor

## Goal

Replace the monolithic `EntsoEDataProcessor.py` with a small package that:

1. Has **one public class** — `EntsoEClient` — that callers import and ask for data.
2. Has **one method per documented endpoint** (mapped 1:1 to the doc pages in `docs/ENTSO-E/`).
3. Keeps internal complexity hidden in private modules that are never imported directly.

---

## Proposed structure

Package lives at **repo root** so any script can `from entsoe import EntsoEClient` without `sys.path` tricks.

```
entsoe/                          ← new package at repo root
  __init__.py                    ← exports only EntsoEClient
  client.py                      ← EntsoEClient — the single public class
  _http.py                       ← HttpClient: rate limiting, retry, logging
  _parsers.py                    ← pure XML→numpy functions (no state, no network)

ENTSO-E/
  EntsoEDataProcessor.py         ← kept as-is until Phase 2, then removed
  data_importer/                 ← updated in Phase 2
  ...

docs/
  entsoe-client/                 ← new, written in Phase 4
    README.md                    ← overview & quick-start
    methods.md                   ← one entry per public method
    return-formats.md            ← array shapes, dtypes, timestamp format
    zones.md                     ← EIC zone codes reference
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

Everything that touches the network in one place. No XML, no numpy.

**Rate limiting**
- Hard cap: 400 requests per 60-second rolling window (ENTSO-E limit)
- Counter resets each minute; if cap reached, sleep until the window expires

**Retry logic (preserved from `_perform_robust_request`)**
- Max 6 attempts per request
- HTTP 429 → sleep **10 minutes** (600 s), then retry (counts as one attempt)
- Network / timeout error → exponential backoff: 2 s, 4 s, 8 s, … (capped at 60 s)
- Non-429 HTTP errors (5xx, etc.) → log and retry with backoff
- After 6 failed attempts → write the date to `skipped_dates.log` and return `None`

**Encoding**
- Always decode with `response.content.decode('utf-8')` — never `response.text`.
  This is the fix for the Latin-1/UTF-8 mismatch that broke Danish PSR names
  (e.g. `å` decoded as `Ã¥`). Must survive the refactor unchanged.

**`skipped_dates.log`**
- Path is configurable at `HttpClient.__init__` (default: `skipped_dates.log` in cwd)
- Append-only; one line per skipped date with ISO timestamp and URL fragment

**Public interface**
```python
class HttpClient:
    def __init__(self, api_key: str, skipped_log: str = "skipped_dates.log"): ...
    def get(self, url: str) -> str | None: ...   # only public method
```

---

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

---

### `client.py` — `EntsoEClient`

One method per endpoint. Internally uses `HttpClient` + parser functions.
All methods share the same signature shape:
- `start` / `end` as `"YYYY-MM-DD"` strings
- `zone` as `"DK1"` / `"DK2"` (client maps to EIC internally)
- Returns a numpy array or `None`

Every public method on `EntsoEClient` gets a short docstring: one line describing what it returns, the doc-page reference (e.g. `16.1.A`), and the document type code. No parameter descriptions needed — those live in `docs/entsoe-client/methods.md`.

```python
def actual_generation_per_unit(self, zone: str, psr_name: str, ...) -> np.ndarray | None:
    """Return 15-min actual generation for a single unit (A73, doc 16.1.A)."""
```

Additional parameters where needed:
- `registered_resource: str | None` on `actual_generation_per_unit` — bypasses XML name
  matching for units with non-ASCII names (see DK1/DK2 unit code docs)
- `pad_missing_days: bool`, `fill_value` — same semantics as current processor

---

## Endpoint methods (mapped to docs)

### Generation (`docs/ENTSO-E/Generation/`)

| Method | Doc page | Document type | Status |
|---|---|---|---|
| `actual_generation_per_unit(zone, psr_name, start, end, registered_resource=None)` | 16.1.A | A73 | implement |
| `actual_generation_per_type(zone, production_types, start, end)` | 16.1.B&C | A75 | implement |
| `installed_capacity_per_type(zone, start, end)` | 14.1.A | A68 | stub |
| `installed_capacity_per_unit(zone, start, end)` | 14.1.B | A71 | stub |
| `day_ahead_generation_forecast(zone, start, end)` | 14.1.C | A69 | stub |
| `wind_solar_forecast(zone, start, end)` | 14.1.D | A69 | stub |

### Transmission (`docs/ENTSO-E/Transmission/`)

| Method | Doc page | Document type | Status |
|---|---|---|---|
| `physical_flow(from_zone, to_zone, start, end)` | 12.1.G | A11 | implement |
| `forecasted_transfer_capacity(from_zone, to_zone, start, end)` | 11.1.A | A61 | stub |

### Load (`docs/ENTSO-E/Load/`)

| Method | Doc page | Document type | Status |
|---|---|---|---|
| `actual_total_load(zone, start, end)` | 6.1.A | A65 | stub |
| `day_ahead_load_forecast(zone, start, end)` | 6.1.B | A65 | stub |

> Stubs raise `NotImplementedError` with a message pointing to the doc page. They establish the full public surface without forcing implementation upfront.

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
- Create `entsoe/__init__.py`, `_http.py`, `_parsers.py`, `client.py` at repo root
- Move code out of `EntsoEDataProcessor.py` method-by-method, **no logic changes**
- `EntsoEDataProcessor.py` temporarily re-exports `EntsoEClient` as `EntsoeDataProcessor` for backward compatibility
- Verify: all existing scripts (`ENTSO-E/solar_4farms_2025_2026.py`, `ENTSO-E/dk_solar_wind_2.py`, `ENTSO-E/data_importer/importer.py`) still import and run unchanged

### Phase 2 — Update callers
- Update the three scripts above to `from entsoe import EntsoEClient`
- Update `data_importer/importer.py` to drop the `sys.path` hack — it can now do a clean top-level import
- Remove the backward-compat shim from `EntsoEDataProcessor.py`
- Delete `EntsoEDataProcessor.py`

### Phase 3 — Add stub methods
- Add `NotImplementedError` stubs for all documented endpoints not yet implemented
- No new fetching logic — just establishes the public API surface

### Phase 4 — Write `docs/entsoe-client/`
- Create `docs/entsoe-client/README.md` — overview, install/config, quick-start example
- Create `docs/entsoe-client/methods.md` — one section per public method: signature, params, return shape, example call
- Create `docs/entsoe-client/return-formats.md` — array shapes, dtypes, timestamp string format (`YYYYMMDDH`), 96-rows-per-day convention
- Create `docs/entsoe-client/zones.md` — EIC zone code reference table, note on ENTSO-E day boundaries (22:00–22:00 UTC offset)

---

## What stays the same

- All numpy array shapes and dtypes are unchanged
- `pad_hourly_to_15min` logic is unchanged (PT60M → PT15M by repeating each value 4×)
- `time_hour_minute="0000"` convention for DK solar/wind stays
- `skipped_dates.log` behaviour and format stay — only the path becomes configurable
- Rate limit constants stay (400 req/min, 10-min sleep on 429)
- `response.content.decode('utf-8')` encoding fix stays — must not regress
