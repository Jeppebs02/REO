# IF aFRR 3.16 Cross Border Marginal Prices (CBMPs) for aFRR Central Selection (CS)

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/balancing/r3/cbmpsForAfrrStandardProduct/show](https://transparency.entsoe.eu/balancing/r3/cbmpsForAfrrStandardProduct/show)
### Method: GET
>```
>{{baseUrl}}?documentType=A84&processType=A67&businessType=A96&Standard_MarketProduct=A01&controlArea_Domain=10YDE-VE-------2&periodStart=202311082300&periodEnd=202311092300
>```
### Query Params

|Param|value|
|---|---|
|documentType|A84|
|processType|A67|
|businessType|A96|
|Standard_MarketProduct|A01|
|controlArea_Domain|10YDE-VE-------2|
|periodStart|202311082300|
|periodEnd|202311092300|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="utf-8"?>
<Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:1">
    <mRID>84d7d4cf6bbe4690b2c9960f1c948652</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A84</type>
    <process.processType>A67</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2023-11-09T12:34:50Z</createdDateTime>
    <area_Domain.mRID codingScheme="A01">10YDE-VE-------2</area_Domain.mRID>
    <period.timeInterval>
        <start>2023-11-08T23:00Z</start>
        <end>2023-11-09T03:00Z</end>
    </period.timeInterval>
    <TimeSeries>
        <mRID>1</mRID>
        <businessType>A96</businessType>
        <standard_MarketProduct.marketProductType>A01</standard_MarketProduct.marketProductType>
        <mktPSRType.psrType>A03</mktPSRType.psrType>
        <flowDirection.direction>A02</flowDirection.direction>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A03</curveType>
        <Period>
            <timeInterval>
                <start>2023-11-08T23:00Z</start>
                <end>2023-11-09T03:00Z</end>
            </timeInterval>
            <resolution>PT4S</resolution>
            <Point>
                <position>1</position>
            </Point>
            <Point>
                <position>2</position>
                <activation_Price.amount>19.12</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>3</position>
                <activation_Price.amount>32.19</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>4</position>
                <activation_Price.amount>25.64</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>5</position>
                <activation_Price.amount>51</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>6</position>
                <activation_Price.amount>52.38</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>7</position>
                <activation_Price.amount>52.84</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>8</position>
                <activation_Price.amount>52.89</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>9</position>
                <activation_Price.amount>52.93</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>10</position>
                <activation_Price.amount>52.89</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>11</position>
                <activation_Price.amount>52.97</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>12</position>
                <activation_Price.amount>52.93</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>13</position>
                <activation_Price.amount>52.96</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>
                <position>15</position>
                <activation_Price.amount>93.25</activation_Price.amount>
                <imbalance_Price.category>A08</imbalance_Price.category>
            </Point>
            <Point>

... (truncated 4780 of 4880 lines — see the original 'Transparency Platform Restful API.md' for the full example)
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
