# IFs mFRR 9.9, aFRR 9.6&9.8 Changes to Bid Availability

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/balancing/r3/changesToBidAvailability/show](https://transparency.entsoe.eu/balancing/r3/changesToBidAvailability/show)
### Method: GET
>```
>{{baseUrl}}?documentType=B45&processType=A47&Domain=10YDE-VE-------2&periodStart=202309232200&periodEnd=202309242200
>```
### Query Params

|Param|value|
|---|---|
|documentType|B45|
|processType|A47|
|Domain|10YDE-VE-------2|
|periodStart|202309232200|
|periodEnd|202309242200|
|businessType|C46|
|offset|100|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="UTF-8"?>
<BidAvailability_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-n:bidavailabilitydocument:1:0">
    <mRID>548bdf3a779a477cbe308e12efa2fcdf</mRID>
    <revisionNumber>1</revisionNumber>
    <type>B45</type>
    <process.processType>A47</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2023-12-03T13:11:45Z</createdDateTime>
    <time_Period.timeInterval>
        <start>2023-09-24T10:45Z</start>
        <end>2023-09-24T11:00Z</end>
    </time_Period.timeInterval>
    <BidTimeSeries>
        <mRID>lWRRK3XB80WYp1kiJgqOMZ</mRID>
        <bidDocument_MarketDocument.mRID>BID_20230924_1245_50Hertz</bidDocument_MarketDocument.mRID>
        <bidDocument_MarketDocument.revisionNumber>1</bidDocument_MarketDocument.revisionNumber>
        <requestingParty_MarketParticipant.mRID codingScheme="A01">10XDE-EON-NETZ-C</requestingParty_MarketParticipant.mRID>
        <requestingParty_MarketParticipant.marketRole.type>A49</requestingParty_MarketParticipant.marketRole.type>
        <businessType>C46</businessType>
        <domain.mRID codingScheme="A01">10YDE-VE-------2</domain.mRID>
        <Reason>
            <code>B47</code>
            <text>!@#$%^*-_=+,./?\|`~[]{}:"';</text>
        </Reason>
    </BidTimeSeries>
</BidAvailability_MarketDocument>
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
