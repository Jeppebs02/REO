# IFs 3.10, 3.16 & 3.17 Netted and Exchanged Volumes

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/balancing/r3/nettedVolumesAndNetPositions/show](https://transparency.entsoe.eu/balancing/r3/nettedVolumesAndNetPositions/show)
### Method: GET
>```
>{{baseUrl}}?documentType=B17&processType=A63&Acquiring_Domain=10YDE-VE-------2&Connecting_Domain=10YDE-VE-------2&periodStart=202301012300&periodEnd=202301022300
>```
### Query Params

|Param|value|
|---|---|
|documentType|B17|
|processType|A63|
|Acquiring_Domain|10YDE-VE-------2|
|Connecting_Domain|10YDE-VE-------2|
|periodStart|202301012300|
|periodEnd|202301022300|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
[001-NETTED_AND_EXCHANGED_VOLUMES_201512312300-201601022300.xml] (indicates zip file entry)

<?xml version="1.0" encoding="utf-8"?>
<Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:1">
    <mRID>4dccc175309a4867aad5e59d79b624b7</mRID>
    <revisionNumber>1</revisionNumber>
    <type>B17</type>
    <process.processType>A63</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2023-09-18T14:54:49Z</createdDateTime>
    <docStatus>
        <value>A02</value>
    </docStatus>
    <area_Domain.mRID codingScheme="A01">10Y1001C--00119X</area_Domain.mRID>
    <period.timeInterval>
        <start>2023-01-01T23:00Z</start>
        <end>2023-01-02T23:00Z</end>
    </period.timeInterval>
    <TimeSeries>
        <mRID>1</mRID>
        <businessType>B09</businessType>
        <acquiring_Domain.mRID codingScheme="A01">10YDE-VE-------2</acquiring_Domain.mRID>
        <connecting_Domain.mRID codingScheme="A01">10Y1001C--00119X</connecting_Domain.mRID>
        <quantity_Measure_Unit.name>MWH</quantity_Measure_Unit.name>
        <curveType>A01</curveType>
        <Period>
            <timeInterval>
                <start>2023-01-02T09:30Z</start>
                <end>2023-01-02T10:00Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>8.3617</quantity>
            </Point>
            <Point>
                <position>2</position>
                <quantity>3.5581</quantity>
            </Point>
        </Period>
        <Period>
            <timeInterval>
                <start>2023-01-02T11:30Z</start>
                <end>2023-01-02T11:45Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>0.014</quantity>
            </Point>
        </Period>
        <Period>
            <timeInterval>
                <start>2023-01-02T12:30Z</start>
                <end>2023-01-02T12:45Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>0.3889</quantity>
            </Point>
        </Period>
        <Period>
            <timeInterval>
                <start>2023-01-02T14:15Z</start>
                <end>2023-01-02T14:45Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>1.2194</quantity>
            </Point>
            <Point>
                <position>2</position>
                <quantity>0.6776</quantity>
            </Point>
        </Period>
        <Period>
            <timeInterval>
                <start>2023-01-02T15:15Z</start>
                <end>2023-01-02T15:45Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>13.3519</quantity>
            </Point>
            <Point>
                <position>2</position>
                <quantity>1.977</quantity>
            </Point>
        </Period>
        <Period>
            <timeInterval>
                <start>2023-01-02T16:00Z</start>
                <end>2023-01-02T16:15Z</end>
            </timeInterval>

... (truncated 413 of 513 lines — see the original 'Transparency Platform Restful API.md' for the full example)
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
