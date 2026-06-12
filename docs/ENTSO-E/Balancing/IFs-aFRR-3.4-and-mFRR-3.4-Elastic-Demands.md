# IFs aFRR 3.4 & mFRR 3.4 Elastic Demands

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

### Method: GET
>```
>{{baseUrl}}?documentType=A37&businessType=B75&processType=A47&Acquiring_Domain=10YCZ-CEPS-----N&periodStart=202311302300&periodEnd=202312012300
>```
### Query Params

|Param|value|
|---|---|
|documentType|A37|
|businessType|B75|
|processType|A47|
|Acquiring_Domain|10YCZ-CEPS-----N|
|periodStart|202311302300|
|periodEnd|202312012300|
|offset|0|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
    <mRID>fc685b6b7fe44c7fb800f36151e9aaa5</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A37</type>
    <process.processType>A47</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2023-12-03T13:21:45Z</createdDateTime>
    <reserveBid_Period.timeInterval>
        <start>2023-12-01T20:15Z</start>
        <end>2023-12-01T20:30Z</end>
    </reserveBid_Period.timeInterval>
    <domain.mRID codingScheme="A01">10Y1001C--00085O</domain.mRID>
    <subject_MarketParticipant.mRID codingScheme="A01">10X1001C--00009H</subject_MarketParticipant.mRID>
    <subject_MarketParticipant.marketRole.type>A35</subject_MarketParticipant.marketRole.type>
    <Bid_TimeSeries>
        <mRID>TS_NEED_CEPS_DOWN</mRID>
        <auction.mRID>AUCTION-mFRR</auction.mRID>
        <businessType>B75</businessType>
        <acquiring_Domain.mRID codingScheme="A01">10YCZ-CEPS-----N</acquiring_Domain.mRID>
        <connecting_Domain.mRID codingScheme="A01">10Y1001C--00085O</connecting_Domain.mRID>
        <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <divisible>A01</divisible>
        <flowDirection.direction>A02</flowDirection.direction>
        <energyPrice_Measure_Unit.name>MWH</energyPrice_Measure_Unit.name>
        <standard_MarketProduct.marketProductType>A01</standard_MarketProduct.marketProductType>
        <Period>
            <timeInterval>
                <start>2023-12-01T20:15Z</start>
                <end>2023-12-01T20:30Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <quantity.quantity>50</quantity.quantity>
                <energy_Price.amount>-60</energy_Price.amount>
            </Point>
        </Period>
    </Bid_TimeSeries>
</ReserveBid_MarketDocument>
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
