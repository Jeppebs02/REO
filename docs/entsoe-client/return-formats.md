# Return Formats

All methods return a NumPy array or `None`. This page documents the exact shape, dtype, and content of each return type.

---

## `actual_generation_per_unit` → 2D object array

**Shape:** `(N, 2)` where N = number of 15-minute intervals in the requested range.

**dtype:** `object`

**Columns:**

| Index | Content | Example |
|---|---|---|
| `[:, 0]` | Timestamp string `"YYYYMMDDH"` | `"2025010100"` |
| `[:, 1]` | Generation in MW as `float` | `42.3` |

**Rows per day:** 96 (one per 15-minute interval).

**Timestamp format note:** The hour part (`H`) is two digits but represents the *start of the hour*, not a 15-minute slot. Four consecutive rows share the same timestamp — they all fall within that hour. Example for 2025-01-01 00:00–00:45:

```
["2025010100", 5.2]
["2025010100", 5.2]
["2025010100", 5.2]
["2025010100", 5.2]
["2025010101", 6.0]
...
```

This is because ENTSO-E originally reports hourly data (PT60M), which `EntsoEClient` pads to 15-minute resolution by repeating each hourly value 4×. Native PT15M data already arrives at 15-min resolution and is not repeated.

**Reading the array:**

```python
timestamps = data[:, 0].astype(str)   # string timestamps
quantities = data[:, 1].astype(float) # MW values
```

---

## `actual_generation_per_type` → 2D float64 array

**Shape:** `(N*96, K)` where N = number of days, K = `len(production_types)`.

**dtype:** `float64`

**Columns:** Ordered by the `production_types` list you pass. Column 0 = `production_types[0]`, column 1 = `production_types[1]`, etc.

**Rows per day:** 96.

**Missing types:** If a psrType code is not present in the API response for a given day, its column is filled with zeros for that day — not `NaN`.

**No timestamp column.** The array has no timestamp dimension — row 0 corresponds to the first 15-minute interval of `start`, row 95 to the last interval of `start`, row 96 to the first interval of `start + 1 day`, and so on.

```python
# Build an index if needed
from datetime import datetime, timedelta

start = datetime(2025, 1, 1)
intervals = [start + timedelta(minutes=15*i) for i in range(mix.shape[0])]
```

---

## `physical_flow` → 1D float64 array

**Shape:** `(N*96,)` where N = number of days.

**dtype:** `float64`

**Values:** MW. Positive = flow in the direction `from_zone` → `to_zone`. To get net import/export, fetch both directions and subtract:

```python
net_import_dk1 = client.physical_flow("DE", "DK1", start, end) \
               - client.physical_flow("DK1", "DE", start, end)
```

**Rows per day:** 96. No timestamp column — same indexing convention as `actual_generation_per_type`.

---

## `None` return

Any method returns `None` if:

- The date format is invalid.
- Every day in the range failed after all retries and `pad_missing_days=False`.
- An unexpected exception occurred before any data was accumulated.

Always guard the return value:

```python
data = client.actual_generation_per_unit(...)
if data is None:
    print("Fetch failed — check skipped_dates.log")
```
