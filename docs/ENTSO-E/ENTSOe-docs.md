# ENTSO-E Transparency Platform RESTful API

Documentation for the ENTSO-E Transparency Platform RESTful API, split from the
single Postman export (`Transparency Platform Restful API.md`) into one file per
endpoint, grouped by collection. Each endpoint page contains the HTTP method,
query parameters, and a sample response. Very long sample payloads are truncated;
the full examples remain in `Transparency Platform Restful API.md`.

## Collections

| Collection | Endpoints | Scope |
|---|---|---|
| [Market](./Market/README.md) | 14 | Allocations, transfer capacity, congestion income, energy prices |
| [Load](./Load/README.md) | 7 | Actual load and load forecasts (day/week/month/year-ahead) |
| [Generation](./Generation/README.md) | 7 | Installed capacity, actual generation, generation forecasts |
| [Transmission](./Transmission/README.md) | 9 | Cross-border flows, commercial schedules, redispatching, countertrading |
| [Outages](./Outages/README.md) | 6 | Unavailability of production, generation, consumption and grid infrastructure |
| [Balancing](./Balancing/README.md) | 35 | Reserves, activated balancing energy, imbalance prices, FCR/FRR/RR |
| [Master Data](./Master-Data/README.md) | 1 | Production and generation units reference data |
| [OMI](./OMI/README.md) | 1 | Other market information |

Each collection folder has a `README.md` index listing its endpoints. Click a
collection above to browse its endpoints.

## How the docs are organised

```
docs/ENTSO-E/
├── ENTSOe-docs.md                       ← this overview
├── Transparency Platform Restful API.md ← original full export (reference, with complete examples)
├── Market/
│   ├── README.md                        ← index of Market endpoints
│   └── <endpoint>.md                    ← one file per endpoint
├── Load/
├── Generation/
├── Transmission/
├── Outages/
├── Balancing/
├── Master-Data/
└── OMI/
```

## External references

Link to Postman collection docs:

https://documenter.getpostman.com/view/7009892/2s93JtP3F6

Link to the same docs but on a website:

https://transparencyplatform.zendesk.com/hc/en-us/sections/12783116987028-Web-API

Link to Sitemap for Restful API Integration:

https://transparencyplatform.zendesk.com/hc/en-us/articles/15692855254548-Sitemap-for-Restful-API-Integration
