# Production and Generation Units

> Part of the [Master Data](./README.md) collection · [API overview](../ENTSOe-docs.md)

> Response contains commissioned production units for given day
### Method: GET
>```
>{{baseUrl}}?documentType=A95&businessType=B11&BiddingZone_Domain=10YBE----------2&Implementation_DateAndOrTime=2017-01-01
>```
### Query Params

|Param|value|
|---|---|
|documentType|A95|
|businessType|B11|
|BiddingZone_Domain|10YBE----------2|
|Implementation_DateAndOrTime|2017-01-01|
|psrType|B04|


### Response: 200
<details open style="width: fit-content; max-height: 600px; overflow: auto">
<summary>Response example:</summary>

```json
<?xml version="1.0" encoding="UTF-8"?> 
<Configuration_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:configurationdocument:3:0">
    <mRID>e52e35a2c0844e8ca98cd46999f0e39d</mRID>
    <type>A95</type>
    <process.processType>A39</process.processType>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A32</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2023-09-06T10:38:25Z</createdDateTime>
    <TimeSeries>
        <mRID>b362105694c34556</mRID>
        <businessType>B11</businessType>
        <implementation_DateAndOrTime.date>2014-10-01</implementation_DateAndOrTime.date>
        <biddingZone_Domain.mRID codingScheme="A01">10YBE----------2</biddingZone_Domain.mRID>
        <registeredResource.mRID codingScheme="A01">22WSAINT-000221B</registeredResource.mRID>
        <registeredResource.name>SAINT-GHISLAIN STEG</registeredResource.name>
        <registeredResource.location.name>Belgium</registeredResource.location.name>
        <ControlArea_Domain>
            <mRID codingScheme="A01">10YBE----------2</mRID>
        </ControlArea_Domain>
        <Provider_MarketParticipant>
            <mRID codingScheme="A01">10X1001A1001A094</mRID>
        </Provider_MarketParticipant>
        <MktPSRType>
            <psrType>B04</psrType>
            <production_PowerSystemResources.highVoltageLimit unit="KVT">150</production_PowerSystemResources.highVoltageLimit>
            <nominalIP_PowerSystemResources.nominalP unit="MAW">350</nominalIP_PowerSystemResources.nominalP>
            <GeneratingUnit_PowerSystemResources>
                <mRID codingScheme="A01">22WSAINT-150221B</mRID>
                <name>SAINT-GHISLAIN STEG</name>
                <nominalP unit="MAW">350</nominalP>
                <generatingUnit_PSRType.psrType>B04</generatingUnit_PSRType.psrType>
                <generatingUnit_Location.name>Belgium</generatingUnit_Location.name>
            </GeneratingUnit_PowerSystemResources>
        </MktPSRType>
    </TimeSeries>
    <TimeSeries>
        <mRID>caaf8805343340ff</mRID>
        <businessType>B11</businessType>
        <implementation_DateAndOrTime.date>2016-07-01</implementation_DateAndOrTime.date>
        <biddingZone_Domain.mRID codingScheme="A01">10YBE----------2</biddingZone_Domain.mRID>
        <registeredResource.mRID codingScheme="A01">22WDOELX40000793</registeredResource.mRID>
        <registeredResource.name>DOEL 4</registeredResource.name>
        <registeredResource.location.name>Belgium</registeredResource.location.name>
        <ControlArea_Domain>
            <mRID codingScheme="A01">10YBE----------2</mRID>
        </ControlArea_Domain>
        <Provider_MarketParticipant>
            <mRID codingScheme="A01">10X1001A1001A094</mRID>
        </Provider_MarketParticipant>
        <MktPSRType>
            <psrType>B14</psrType>
            <production_PowerSystemResources.highVoltageLimit unit="KVT">380</production_PowerSystemResources.highVoltageLimit>
            <nominalIP_PowerSystemResources.nominalP unit="MAW">1039</nominalIP_PowerSystemResources.nominalP>
            <GeneratingUnit_PowerSystemResources>
                <mRID codingScheme="A01">22WDOELX41500793</mRID>
                <name>DOEL 4</name>
                <nominalP unit="MAW">1039</nominalP>
                <generatingUnit_PSRType.psrType>B14</generatingUnit_PSRType.psrType>
                <generatingUnit_Location.name>Belgium</generatingUnit_Location.name>
            </GeneratingUnit_PowerSystemResources>
        </MktPSRType>
    </TimeSeries>
    <TimeSeries>
        <mRID>d70176d8ff70447c</mRID>
        <businessType>B11</businessType>
        <implementation_DateAndOrTime.date>2014-01-01</implementation_DateAndOrTime.date>
        <biddingZone_Domain.mRID codingScheme="A01">10YBE----------2</biddingZone_Domain.mRID>
        <registeredResource.mRID codingScheme="A01">22WCOOXII000070C</registeredResource.mRID>
        <registeredResource.name>COO II T</registeredResource.name>
        <registeredResource.location.name>Belgium</registeredResource.location.name>
        <ControlArea_Domain>
            <mRID codingScheme="A01">10YBE----------2</mRID>
        </ControlArea_Domain>
        <Provider_MarketParticipant>
            <mRID codingScheme="A01">10X1001A1001A094</mRID>
        </Provider_MarketParticipant>
        <MktPSRType>
            <psrType>B10</psrType>
            <production_PowerSystemResources.highVoltageLimit unit="KVT">380</production_PowerSystemResources.highVoltageLimit>
            <nominalIP_PowerSystemResources.nominalP unit="MAW">690</nominalIP_PowerSystemResources.nominalP>
            <GeneratingUnit_PowerSystemResources>
                <mRID codingScheme="A01">22WCOOX5X000061A</mRID>
                <name>COO 5 T</name>
                <nominalP unit="MAW">230</nominalP>
                <generatingUnit_PSRType.psrType>B10</generatingUnit_PSRType.psrType>
                <generatingUnit_Location.name>Belgium</generatingUnit_Location.name>
            </GeneratingUnit_PowerSystemResources>
            <GeneratingUnit_PowerSystemResources>
                <mRID codingScheme="A01">22WCOOX6X000064W</mRID>
                <name>COO 6 T</name>
                <nominalP unit="MAW">230</nominalP>
                <generatingUnit_PSRType.psrType>B10</generatingUnit_PSRType.psrType>
                <generatingUnit_Location.name>Belgium</generatingUnit_Location.name>
            </GeneratingUnit_PowerSystemResources>
            <GeneratingUnit_PowerSystemResources>
                <mRID codingScheme="A01">22WCOOX4X0000588</mRID>
                <name>COO 4 T</name>
                <nominalP unit="MAW">230</nominalP>

... (truncated 784 of 884 lines — see the original 'Transparency Platform Restful API.md' for the full example)
```
</details>


⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃ ⁃
