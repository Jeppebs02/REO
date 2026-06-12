# EntsoEClient

`EntsoEClient` is the single public class for fetching electricity market data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu). It lives in the `entsoe/` package at the repo root.

## Setup

**Requires an ENTSO-E API key.** Register at transparency.entsoe.eu → My Account Settings → Web API Security Token.

Set the key as an environment variable:

```bash
# Windows
set API_KEY=your-key-here

# PowerShell
$env:API_KEY = "your-key-here"
```

**Install dependencies** (same as the rest of the project):

```bash
pip install requests numpy
```

## Quick start

```python
import os
from entsoe import EntsoEClient

client = EntsoEClient(api_key=os.getenv("API_KEY"))

# 15-min solar generation for a single farm
data = client.actual_generation_per_unit(
    zone="DK1",
    psr_name="Solar Park Kassoe",
    start="2025-01-01",
    end="2025-03-31",
    time_hour_minute="0000",   # midnight-to-midnight window for DK solar
    pad_missing_days=True,
    fill_value=0,
)
# data.shape → (8928, 2)   [93 days × 96 intervals, columns: timestamp + MW]

# 15-min generation mix for a zone
mix = client.actual_generation_per_type(
    zone="DK1",
    production_types=["B01", "B05", "B16", "B18", "B19"],
    start="2025-01-01",
    end="2025-03-31",
)
# mix.shape → (8928, 5)   [93 days × 96 intervals, one column per type]

# 15-min physical flow between two zones
flow = client.physical_flow(
    from_zone="DE",
    to_zone="DK1",
    start="2025-01-01",
    end="2025-03-31",
)
# flow.shape → (8928,)   [93 days × 96 intervals, MW]
```

## Error handling

- Returns `None` if the entire fetch fails (network down, all retries exhausted).
- Fills individual missing days with `fill_value` when `pad_missing_days=True` (default: `False`).
- Skipped dates are appended to `skipped_dates.log` in the working directory. The log path is configurable: `EntsoEClient(api_key=..., skipped_log="path/to/log.txt")`.

## Rate limiting

The client self-manages ENTSO-E's 400 req/min limit. On HTTP 429 it sleeps 10 minutes automatically. No manual throttling needed.

## Further reading

- [methods.md](methods.md) — full parameter reference for every method
- [return-formats.md](return-formats.md) — array shapes, dtypes, timestamp format
- [zones.md](zones.md) — zone shortcodes, EIC codes, day-boundary quirks
