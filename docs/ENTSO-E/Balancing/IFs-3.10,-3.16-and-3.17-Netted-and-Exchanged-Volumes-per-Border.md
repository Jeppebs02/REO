# IFs 3.10, 3.16 & 3.17 Netted and Exchanged Volumes per Border

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/balancing/r3/nettedVolumesAndNetPositions/show](https://transparency.entsoe.eu/balancing/r3/nettedVolumesAndNetPositions/show)
### Method: GET
>```
>{{baseUrl}}?documentType=A30&processType=A60&Acquiring_Domain=10YBE----------2&Connecting_Domain=10YFR-RTE------C&periodStart=202503010000&periodEnd=202503020000
>```
### Query Params

|Param|value|
|---|---|
|documentType|A30|
|processType|A60|
|Acquiring_Domain|10YBE----------2|
|Connecting_Domain|10YFR-RTE------C|
|periodStart|202503010000|
|periodEnd|202503020000|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="utf-8"?>
<Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:1">
    <mRID>dbdf5933133c4246b5fa2caec33be43f</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A30</type>
    <process.processType>A60</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A35</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A32</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2025-04-11T07:15:42Z</createdDateTime>
    <area_Domain.mRID codingScheme="A01">10Y1001C--00085O</area_Domain.mRID>
    <period.timeInterval>
        <start>2025-03-01T00:00Z</start>
        <end>2025-03-02T00:00Z</end>
    </period.timeInterval>
    <TimeSeries>
        <mRID>1</mRID>
        <businessType>A45</businessType>
        <acquiring_Domain.mRID codingScheme="A01">10YBE----------2</acquiring_Domain.mRID>
        <connecting_Domain.mRID codingScheme="A01">10YFR-RTE------C</connecting_Domain.mRID>
        <standard_MarketProduct.marketProductType>A01</standard_MarketProduct.marketProductType>
        <quantity_Measure_Unit.name>MWH</quantity_Measure_Unit.name>
        <curveType>A01</curveType>
        <Period>
            <timeInterval>
                <start>2025-03-01T00:00Z</start>
                <end>2025-03-02T00:00Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>2</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>3</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>4</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>5</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>6</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>7</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>8</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>9</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>10</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>11</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>12</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>13</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>14</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>15</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>16</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>17</position>
                <quantity>0</quantity>
            </Point>
            <Point>
                <position>18</position>

... (truncated 317 of 417 lines — see the original 'Transparency Platform Restful API.md' for the full example)
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
