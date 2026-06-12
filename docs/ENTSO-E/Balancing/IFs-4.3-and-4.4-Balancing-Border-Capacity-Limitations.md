# IFs 4.3 & 4.4 Balancing Border Capacity Limitations

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/balancing/r2/crossBorderCapacityLimits/show](https://transparency.entsoe.eu/balancing/r2/crossBorderCapacityLimits/show)
### Method: GET
>```
>{{baseUrl}}?documentType=A31&BusinessType=A26&processType=A47&Out_Domain=10YCZ-CEPS-----N&In_Domain=10YAT-APG------L&periodStart=202401312300&periodEnd=202402012300
>```
### Query Params

|Param|value|
|---|---|
|documentType|A31|
|BusinessType|A26|
|processType|A47|
|Out_Domain|10YCZ-CEPS-----N|
|In_Domain|10YAT-APG------L|
|periodStart|202401312300|
|periodEnd|202402012300|
|registeredResource|22T201903146---W|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="UTF-8"?>
<Capacity_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:capacitydocument:8:0" >
    <mRID>b6e10b6844a44467b8d0e7ca4efc5216</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A31</type>
    <process.processType>A47</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2024-03-08T07:53:22Z</createdDateTime>
    <period.timeInterval>
        <start>2024-01-31T23:00Z</start>
        <end>2024-02-01T10:00Z</end>
    </period.timeInterval>
    <domain.mRID codingScheme="A01">10Y1001C--00085O</domain.mRID>
    <TimeSeries>
        <mRID>1</mRID>
        <businessType>A26</businessType>
        <product>8716867000016</product>
        <in_Domain.mRID codingScheme="A01">10YAT-APG------L</in_Domain.mRID>
        <out_Domain.mRID codingScheme="A01">10YCZ-CEPS-----N</out_Domain.mRID>
        <measure_Unit.name>MAW</measure_Unit.name>
        <curveType>A01</curveType>
        <Period>
            <timeInterval>
                <start>2024-01-31T23:00Z</start>
                <end>2024-02-01T10:00Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>2</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>3</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>4</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>5</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>6</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>7</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>8</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>9</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>10</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>11</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>12</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>13</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>14</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>15</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>16</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>17</position>
                <quantity>280</quantity>
            </Point>
            <Point>
                <position>18</position>

... (truncated 109 of 209 lines — see the original 'Transparency Platform Restful API.md' for the full example)
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
