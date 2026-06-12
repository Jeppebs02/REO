# Method Reference

All methods are on `EntsoEClient`. Dates are `"YYYY-MM-DD"` strings. Zone accepts a shortcode (`"DK1"`) or a raw EIC code — see [zones.md](zones.md).

---

## `actual_generation_per_unit` — A73, doc 16.1.A

Returns 15-min actual generation for a **single generation unit**.

```python
client.actual_generation_per_unit(
    zone: str,
    psr_name: str,
    start: str,
    end: str,
    registered_resource: str | None = None,
    pad_missing_days: bool = False,
    fill_value = np.nan,
    time_hour_minute: str = "2200",
    base_api_url: str = "https://web-api.tp.entsoe.eu/api",
) -> np.ndarray | None
```

| Parameter | Description |
|---|---|
| `zone` | Bidding zone — `"DK1"`, `"DK2"`, `"DE"`, or raw EIC code |
| `psr_name` | Unit name exactly as it appears in ENTSO-E XML (e.g. `"Solar Park Kassoe"`) |
| `start` / `end` | Date range, inclusive on both ends |
| `registered_resource` | Unit EIC code (e.g. `"45W000000000209G"`). When set, the API filters server-side and `psr_name` is used only for logging — not for XML name matching. Use this for units with non-ASCII names (Danish `å`, `ø`, etc.). See `docs/ENTSO-E/Other/BZN DK1 Generation Units.md` for codes. |
| `pad_missing_days` | If `True`, days with no API data are filled with `fill_value` (96 rows of zeros/nan). Default `False` — missing days are silently skipped. |
| `fill_value` | Value used for padded days. Default `np.nan`. |
| `time_hour_minute` | The HHmm offset that defines the start of each queried period. Default `"2200"` (ENTSO-E convention). Use `"0000"` for DK solar/wind to get midnight-aligned calendar days. |

**Returns:** 2D `dtype=object` array, shape `(N, 2)`. See [return-formats.md](return-formats.md).

**Example — DK solar farm, midnight-aligned, gaps filled with 0:**

```python
data = client.actual_generation_per_unit(
    zone="DK1",
    psr_name="Solar Park Viuf and Håstrup",
    start="2025-01-01",
    end="2025-12-31",
    registered_resource="45W000000000221Q",
    time_hour_minute="0000",
    pad_missing_days=True,
    fill_value=0,
)
```

---

## `actual_generation_per_type` — A75, doc 16.1.B&C

Returns 15-min actual generation aggregated **by production type** for a whole zone.

```python
client.actual_generation_per_type(
    zone: str,
    production_types: list[str],
    start: str,
    end: str,
    base_api_url: str = "https://web-api.tp.entsoe.eu/api",
) -> np.ndarray | None
```

| Parameter | Description |
|---|---|
| `zone` | Bidding zone |
| `production_types` | Ordered list of psrType codes, e.g. `["B01", "B05", "B16"]`. Defines the column order of the output array. Types absent from the API response appear as a column of zeros. |

**Returns:** 2D `dtype=float64` array, shape `(N*96, len(production_types))`. See [return-formats.md](return-formats.md).

**Common psrType codes:**

| Code | Type |
|---|---|
| B01 | Biomass |
| B04 | Fossil Gas |
| B05 | Fossil Hard Coal |
| B10 | Hydro Pumped Storage |
| B11 | Hydro Run-of-river |
| B16 | Solar |
| B18 | Wind Offshore |
| B19 | Wind Onshore |

**Example:**

```python
mix = client.actual_generation_per_type(
    zone="DK1",
    production_types=["B16", "B18", "B19", "B05", "B04"],
    start="2025-01-01",
    end="2025-12-31",
)
solar_col = mix[:, 0]   # B16 — Solar
```

---

## `physical_flow` — A11, doc 12.1.G

Returns 15-min cross-border **physical flow** between two zones (MW, positive = export from `from_zone`).

```python
client.physical_flow(
    from_zone: str,
    to_zone: str,
    start: str,
    end: str,
    base_api_url: str = "https://web-api.tp.entsoe.eu/api",
) -> np.ndarray | None
```

| Parameter | Description |
|---|---|
| `from_zone` | Exporting zone |
| `to_zone` | Importing zone |

**Returns:** 1D `dtype=float64` array, shape `(N*96,)`. See [return-formats.md](return-formats.md).

**Example — import to DK1 from Germany:**

```python
import_mw = client.physical_flow(
    from_zone="DE",
    to_zone="DK1",
    start="2025-01-01",
    end="2025-12-31",
)
export_mw = client.physical_flow(
    from_zone="DK1",
    to_zone="DE",
    start="2025-01-01",
    end="2025-12-31",
)
```

---

## Stub methods (not yet implemented)

These methods exist to establish the full API surface. They raise `NotImplementedError` with a message pointing to the relevant doc page.

| Method | Doc | Document type |
|---|---|---|
| `installed_capacity_per_type(zone, start, end)` | 14.1.A | A68 |
| `installed_capacity_per_unit(zone, start, end)` | 14.1.B | A71 |
| `day_ahead_generation_forecast(zone, start, end)` | 14.1.C | A69 |
| `wind_solar_forecast(zone, start, end)` | 14.1.D | A69 |
| `forecasted_transfer_capacity(from_zone, to_zone, start, end)` | 11.1.A | A61 |
| `actual_total_load(zone, start, end)` | 6.1.A | A65 |
| `day_ahead_load_forecast(zone, start, end)` | 6.1.B | A65 |

To implement a stub, add the fetch loop + parser to `entsoe/client.py` following the pattern of the existing methods.

---

## Constructor

```python
EntsoEClient(api_key: str, skipped_log: str = "skipped_dates.log")
```

| Parameter | Description |
|---|---|
| `api_key` | ENTSO-E Web API security token |
| `skipped_log` | Path to the append-only log file for failed/skipped dates. Relative paths resolve from the working directory at runtime. |
