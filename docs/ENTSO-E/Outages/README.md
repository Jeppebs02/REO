# Outages

Part of the [ENTSO-E Transparency Platform RESTful API](../ENTSOe-docs.md) documentation.

If you would like to learn more about the data items under this domain please go to [this link](https://transparencyplatform.zendesk.com/hc/en-us/articles/12784099471764).

The usage of the new optional attributes **TimeIntervalUpdate** or a combination of **PeriodStartUpdate & PeriodEndUpdate** in the query will fetch only the latest updated version of outages within the specified Outage's Timeinterval and specified TimeIntervalUpdate.

TimeIntervalUpdate corresponds to the 'Updated(UTC)' timestamp in value details of the outage published in the platform. For example, UK outages where query results in getting more than 200 documents even when queried for 2 hrs. If this extra optional parameter is added, the API call will limit the results to fetch the latest updated documents. This will help in not fetching already downloaded outages, which you have queried previously.  
It is important to take area’s timezone into consideration, as well as winter/summer time. For example, consider the day of February 2 2016 in CET. This is during winter time and hence using UTC this day is considered to start on 2016-01-01 at 23:00 and end on 2016-01-02 at 23:00. As another example, the day of July 5 2016 in CET is during summer time and using UTC this day is considered to start at 2016-07-04 at 22:00 and end at 2016-07-05 at 22:00.

In case only periodStart & periodEnd are used (without periodStartUpdate & periodEndUpdate), the **time range is limited to 1 year for periodStart & periodEnd**.  
In case a combination of periodStart & periodEnd and a combination of periodStartUpdate & periodEndUpdate are used, **the time range is limited to 1 year only for the combination of periodStartUpdate & periodEndUpdate** (not for periodStart & periodEnd). 

## Endpoints (6)

- [15.1.C-D Unavailability of Production Units](./15.1.C-D-Unavailability-of-Production-Units.md)
- [15.1.A&B Unavailability of Generation Units](./15.1.AandB-Unavailability-of-Generation-Units.md)
- [7.1.A-B Aggregated Unavailability of Consumption Units](./7.1.A-B-Aggregated-Unavailability-of-Consumption-Units.md)
- [10.1.A&B Unavailability of Transmission Infrastructure](./10.1.AandB-Unavailability-of-Transmission-Infrastructure.md)
- [10.1.C Unavailability of Offshore Grid Infrastructure](./10.1.C-Unavailability-of-Offshore-Grid-Infrastructure.md)
- [Fall-backs [IFs IN 7.2, mFRR 3.11, aFRR 3.10]](./Fall-backs-[IFs-IN-7.2,-mFRR-3.11,-aFRR-3.10].md)
