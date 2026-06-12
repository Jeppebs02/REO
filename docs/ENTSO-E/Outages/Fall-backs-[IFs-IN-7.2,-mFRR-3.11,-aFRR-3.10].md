# Fall-backs [IFs IN 7.2, mFRR 3.11, aFRR 3.10]

> Part of the [Outages](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/outage-domain/r2/unavailabilityInTransmissionGrid/show](https://transparency.entsoe.eu/outage-domain/r2/unavailabilityInTransmissionGrid/show)
### Method: GET
>```
>{{baseUrl}}?documentType=A53&ProcessType=A51&BusinessType=C47&BiddingZone_Domain=10YBE----------2&periodStart=202301010000&periodEnd=202401020000
>```
### Query Params

|Param|value|
|---|---|
|documentType|A53|
|ProcessType|A51|
|BusinessType|C47|
|BiddingZone_Domain|10YBE----------2|
|periodStart|202301010000|
|periodEnd|202401020000|
|DocStatus|A13|
|mRID||


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
[001-FALL_BACKS_202307210000-202307211445.xml] (indicates zip file entry)
<?xml version="1.0" encoding="UTF-8"?>
<Unavailability_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:outagedocument:4:0">
    <mRID>8b3fbad6661b4bcf91a6e5da8f0edb59</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A53</type>
    <process.processType>A51</process.processType>
    <createdDateTime>2023-10-20T20:02:57Z</createdDateTime>
    <sender_MarketParticipant.mRID
            codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID
            codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <unavailability_Time_Period.timeInterval>
        <start>2023-07-20T22:00Z</start>
        <end>2023-07-21T12:45Z</end>
    </unavailability_Time_Period.timeInterval>
    <docStatus>
        <value>A02</value>
    </docStatus>
    <TimeSeries>
        <mRID>1</mRID>
        <businessType>C47</businessType>
        <biddingZone_Domain.mRID codingScheme="A01">10YBE----------2</biddingZone_Domain.mRID>
        <start_DateAndOrTime.date>2023-07-20</start_DateAndOrTime.date>
        <start_DateAndOrTime.time>22:00:00Z</start_DateAndOrTime.time>
        <end_DateAndOrTime.date>2023-07-21</end_DateAndOrTime.date>
        <end_DateAndOrTime.time>12:45:00Z</end_DateAndOrTime.time>
        <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
        <curveType>A03</curveType>
        <Reason>
            <code>B13</code>
            <text>Real time connection lost</text>
        </Reason>
    </TimeSeries>
</Unavailability_MarketDocument>
[002-FALL_BACKS_202308240000-202308240030.xml] (indicates zip file entry)
...
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
