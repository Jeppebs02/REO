# IFs 4.5 Permanent Allocation Limitations to Cross-border Capacity on HVDC Lines

> Part of the [Balancing](./README.md) collection · [API overview](../ENTSOe-docs.md)

Data view page in Transparency Platform:

[https://transparency.entsoe.eu/balancing-domain/r2/fcrSharesOfCapacity/show](https://transparency.entsoe.eu/balancing-domain/r2/fcrSharesOfCapacity/show)
### Method: GET
>```
>{{baseUrl}}?documentType=A99&processType=A63&BusinessType=B06&Out_Domain=10YNL----------L&In_Domain=10YDK-1--------W&periodStart=202101010000&periodEnd=202112310000&registeredResource=10T-DK-NL-000012
>```
### Query Params

|Param|value|
|---|---|
|documentType|A99|
|processType|A63|
|BusinessType|B06|
|Out_Domain|10YNL----------L|
|In_Domain|10YDK-1--------W|
|periodStart|202101010000|
|periodEnd|202112310000|
|registeredResource|10T-DK-NL-000012|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="UTF-8"?>
<HVDCLink_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-8:hvdclinkdocument:1:1" >
    <mRID>a233cf51e316457698466d81b21433f1</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A99</type>
    <process.processType>A63</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2025-05-18T16:47:33Z</createdDateTime>
    <docStatus>
        <value>A05</value>
    </docStatus>
    <domain.mRID codingScheme="A01">10YDOM-REGION-1V</domain.mRID>
    <TimeSeries>
        <mRID>1</mRID>
        <businessType>B06</businessType>
        <product>8716867000016</product>
        <objectAggregation>A09</objectAggregation>
        <connectingLine_RegisteredResource.mRID codingScheme="A01">10T-DK-NL-000012</connectingLine_RegisteredResource.mRID>
        <out_Domain.mRID codingScheme="A01">10YNL----------L</out_Domain.mRID>
        <in_Domain.mRID codingScheme="A01">10YDK-1--------W</in_Domain.mRID>
        <measurement_Unit.name>MAW</measurement_Unit.name>
        <maximumExchange_Quantity.quantity>0</maximumExchange_Quantity.quantity>
        <start_DateAndOrTime.dateTime>2021-06-23T22:00:00Z</start_DateAndOrTime.dateTime>
        <Reason>
            <code>B62</code>
        </Reason>
    </TimeSeries>
</HVDCLink_MarketDocument>
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
