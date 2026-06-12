# Zones

## Shortcodes and EIC codes

`EntsoEClient` accepts either a shortcode or a raw EIC code for any `zone`, `from_zone`, or `to_zone` parameter.

| Shortcode | EIC Code | Area |
|---|---|---|
| `DK1` | `10Y1001A1001A796` | Western Denmark (Jutland + Funen) |
| `DK2` | `10YDK-2--------M` | Eastern Denmark (Zealand + Bornholm) |
| `DE` | `10Y1001A1001A83F` | Germany / Luxembourg |

Any EIC code not listed above can be passed directly as a string. The client passes it through unchanged.

---

## Day boundary convention

ENTSO-E defines calendar days differently depending on the endpoint.

### A73 — Actual Generation per Unit

The time window is set by `time_hour_minute` on each call. Two conventions are used in this repo:

| Convention | `time_hour_minute` | Use case |
|---|---|---|
| Calendar day (midnight) | `"0000"` | DK solar and wind farms — data reported in local clock time |
| ENTSO-E day (22:00 UTC) | `"2200"` | Default; matches the ENTSO-E day definition used by other endpoints |

**Which to use:** If you are fetching solar or wind production for Denmark and want each day's 96 rows to align with the calendar date (00:00–23:45), use `time_hour_minute="0000"`. If the unit reports on the ENTSO-E 22:00 UTC convention, use the default `"2200"`.

When in doubt, run `discover_psr_names.py` with a known date and inspect the `<timeInterval>` values in the XML response.

### A75 — Actual Generation per Type and A11 — Physical Flow

These endpoints always use the 22:00–22:00 UTC day boundary, hardcoded inside `EntsoEClient`. The `start` and `end` parameters you pass are calendar dates; the client maps them to the correct 22:00 UTC windows automatically.

---

## DK1 generation unit codes

Unit codes are needed for `registered_resource` when a unit name contains non-ASCII characters (Danish `å`, `ø`, `æ`). Using the code bypasses XML name matching entirely.

Full list: `docs/ENTSO-E/Other/BZN DK1 Generation Units.md`

Selected units:

| Unit Name | Unit Code |
|---|---|
| Solar Park Holsted | `45W000000000214N` |
| Solar Park Kassoe | `45W000000000209G` |
| Solar Park Gedmosen | `45W000000000210V` |
| Solar Park Viuf and Håstrup | `45W000000000221Q` |
| Vesterhav Nord | *(see BZN DK1 doc)* |
| Vesterhav Syd | *(see BZN DK1 doc)* |
| Horns Rev C | *(see BZN DK1 doc)* |

## DK2 generation unit codes

Full list: `docs/ENTSO-E/Other/BZN DK2 Generation Units.md`

Selected units:

| Unit Name | Unit Code |
|---|---|
| Avedøreværket 1 | `45W000000000029I` |
| Avedøreværket 2 | `45W000000000030X` |
| Amagerværket 4 | `45W000000000113T` |
| Solar Park Vedde | `45W000000000222O` |
| Solar Park Lidsø | `45W000000000223M` |
| Rødsand 1 | `45W000000000044M` |
| DK_KF_AB_GU | `45W000000000126K` |
