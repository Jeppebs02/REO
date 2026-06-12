# Other Market Information

> Part of the [OMI](./README.md) collection · [API overview](../ENTSOe-docs.md)

### Method: GET
>```
>{{baseUrl}}?documentType=B47&ControlArea_Domain=10YDE-EON------1&periodStart=202409232200&periodEnd=202409242200
>```
### Query Params

|Param|value|
|---|---|
|documentType|B47|
|ControlArea_Domain|10YDE-EON------1|
|periodStart|202409232200|
|periodEnd|202409242200|
|DocStatus|A05|
|PeriodStartUpdate|202402221000|
|PeriodEndUpdate|202402231200|
|Offset|12|
|mRID|NDE5ODBiYjFkM2ExMTljYTM5Mzk2ODcxNDFkZDE4MzU=|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
[001-OTHER_MARKET_INFORMATION202409182200-202512152259.xml] (indicates zip file entry)
<?xml version="1.0" encoding="UTF-8"?>
<OtherTransparencyMarketInformation_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-n:otmidocument:1:0">
    <mRID>ZWQ5ODdjZjZhYWFkZDA5Yzk0MThkYjUwOGMxNDgxZTk=</mRID>
    <revisionNumber>1</revisionNumber>
    <type>B47</type>
    <sender_MarketParticipant.mRID codingScheme="A01">10XDE-EON-NETZ-C</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A39</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A39</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2024-10-08T11:34:58Z</createdDateTime>
    <docStatus>
        <value>A05</value>
    </docStatus>
    <publication_DateAndOrTime.dateTime>2024-09-19T09:11:00Z</publication_DateAndOrTime.dateTime>
    <start_DateAndOrTime.dateTime>2024-09-18T10:00</start_DateAndOrTime.dateTime>
    <end_DateAndOrTime.dateTime>2025-12-15T10:59</end_DateAndOrTime.dateTime>
    <reason.code>A95</reason.code>
    <reason.text>Delay in the completion of the DolWin5 grid connection system until probably 15 December 2025</reason.text>
</OtherTransparencyMarketInformation_MarketDocument>
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
_________________________________________________
Powered By: [postman-to-markdown](https://github.com/bautistaj/postman-to-markdown/)
