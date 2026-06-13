
# Manage your contacts in Brevo

Import contacts to Brevo to send email campaigns, SMS, and marketing automation messages. Use the API to create and update contacts programmatically.

Import at least the contact's email address. You can also import additional contact attributes such as first name, last name, birthday, mobile phone number, and other custom fields.

To manage contacts through the Brevo app or import contacts using CSV files, see the [help center tutorial](https://help.brevo.com/hc/en-us/articles/115000719584-Importing-your-contacts-into-SendinBlue).

## Before you start

### Get your API key

Retrieve your API key from [your account settings](https://my.brevo.com/account/settings). Read the [Authentication guide](/docs/authentication-schemes) for details.

### Understand the API

If you're new to the Marketing API, read the [Quickstart](/docs/quickstart) guide.

## Create a contact

Create contacts using the `POST /v3/contacts` endpoint. You can create email contacts, SMS contacts, or both.

### Endpoint

### Request

POST [https://api.brevo.com/v3/contacts](https://api.brevo.com/v3/contacts)

```curl response
curl -X POST https://api.brevo.com/v3/contacts \
     -H "api-key: <apiKey>" \
     -H "Content-Type: application/json" \
     -d '{}'
```

```typescript response
import { BrevoClient } from "@getbrevo/brevo";

async function main() {
    const client = new BrevoClient({
        apiKey: "YOUR_API_KEY_HERE",
    });
    await client.contacts.createContact({});
}
main();

```

```python response
from brevo import Brevo

client = Brevo(
    api_key="YOUR_API_KEY_HERE",
)

client.contacts.create_contact()

```

```php response
<?php

namespace Example;

use Brevo\Brevo;
use Brevo\Contacts\Requests\CreateContactRequest;

$client = new Brevo(
    apiKey: 'YOUR_API_KEY_HERE',
);
$client->contacts->createContact(
    new CreateContactRequest([]),
);

```

```go response
package main

import (
	"fmt"
	"strings"
	"net/http"
	"io"
)

func main() {

	url := "https://api.brevo.com/v3/contacts"

	payload := strings.NewReader("{}")

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("api-key", "<apiKey>")
	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))

}
```

```ruby response
require 'uri'
require 'net/http'

url = URI("https://api.brevo.com/v3/contacts")

http = Net::HTTP.new(url.host, url.port)
http.use_ssl = true

request = Net::HTTP::Post.new(url)
request["api-key"] = '<apiKey>'
request["Content-Type"] = 'application/json'
request.body = "{}"

response = http.request(request)
puts response.read_body
```

```java response
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

HttpResponse<String> response = Unirest.post("https://api.brevo.com/v3/contacts")
  .header("api-key", "<apiKey>")
  .header("Content-Type", "application/json")
  .body("{}")
  .asString();
```

```csharp response
using RestSharp;

var client = new RestClient("https://api.brevo.com/v3/contacts");
var request = new RestRequest(Method.POST);
request.AddHeader("api-key", "<apiKey>");
request.AddHeader("Content-Type", "application/json");
request.AddParameter("application/json", "{}", ParameterType.RequestBody);
IRestResponse response = client.Execute(request);
```

```swift response
import Foundation

let headers = [
  "api-key": "<apiKey>",
  "Content-Type": "application/json"
]
let parameters = [] as [String : Any]

let postData = JSONSerialization.data(withJSONObject: parameters, options: [])

let request = NSMutableURLRequest(url: NSURL(string: "https://api.brevo.com/v3/contacts")! as URL,
                                        cachePolicy: .useProtocolCachePolicy,
                                    timeoutInterval: 10.0)
request.httpMethod = "POST"
request.allHTTPHeaderFields = headers
request.httpBody = postData as Data

let session = URLSession.shared
let dataTask = session.dataTask(with: request as URLRequest, completionHandler: { (data, response, error) -> Void in
  if (error != nil) {
    print(error as Any)
  } else {
    let httpResponse = response as? HTTPURLResponse
    print(httpResponse)
  }
})

dataTask.resume()
```

### Request parameters

Create different types of contacts:

### Schema (`request`)

```yaml
openapi: 3.1.0
info:
  title: API
  version: 1.0.0
paths:
  /contacts:
    post:
      operationId: create-contact
      summary: Create a contact
      description: >-
        <Note>Follow this format when passing a "SMS" phone number as an
        attribute.

        Accepted Number Formats 91xxxxxxxxxx +91xxxxxxxxxx 0091xxxxxxxxxx</Note>

        Creates new contacts on Brevo. Contacts can be created by passing either
        - <br /><br /> 1. email address of the contact (email_id),  <br /> 2. phone
        number of the contact (to be passed as "SMS" field in "attributes" along
        with proper country code), For example- {"SMS":"+91xxxxxxxxxx"} or
        {"SMS":"0091xxxxxxxxxx"} <br /> 3. ext_id <br />
      tags:
        - subpackage_contacts
      responses:
        '201':
          description: Contact created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Contacts_createContact_Response_201'
        '400':
          description: bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/contactErrorModel'
        '425':
          description: Too Early
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/contactErrorModel'
      requestBody:
        description: Values to create a contact
        content:
          application/json:
            schema:
              type: object
              properties:
                attributes:
                  type: object
                  additionalProperties:
                    $ref: >-
                      #/components/schemas/ContactsPostRequestBodyContentApplicationJsonSchemaAttributes
                  description: >
                    Pass the set of attributes and their values. The attribute's
                    parameter should be passed in capital letter while creating
                    a contact. Values that don't match the attribute type (e.g.
                    text or string in a date attribute) will be ignored. **These
                    attributes must be present in your Brevo account**. For eg:
                    **{"FNAME":"Elly", "LNAME":"Roger", "COUNTRIES":
                    ["India","China"]}**
                email:
                  type: string
                  format: email
                  description: >
                    Email address of the user. **Mandatory if "ext_id"  & "SMS"
                    field is not passed.**
                emailBlacklisted:
                  type: boolean
                  description: >-
                    Set this field to blacklist the contact for emails
                    (emailBlacklisted = true)
                ext_id:
                  type: string
                  description: Pass your own Id to create a contact.
                listIds:
                  type: array
                  items:
                    type: integer
                    format: int64
                  description: Ids of the lists to add the contact to
                smsBlacklisted:
                  type: boolean
                  description: >-
                    Set this field to blacklist the contact for SMS
                    (smsBlacklisted = true)
                smtpBlacklistSender:
                  type: array
                  items:
                    type: string
                    format: email
                  description: >-
                    transactional email forbidden sender for contact. Use only
                    for email Contact ( only available if updateEnabled = true )
                updateEnabled:
                  type: boolean
                  default: false
                  description: >-
                    Facilitate to update the existing contact in the same
                    request (updateEnabled = true)
                forceMerge:
                  type: boolean
                  default: false
                  description: >-
                    When true, if the contact being created shares an identifier
                    (email, SMS, ext_id, whatsapp, landline) with an existing
                    contact, the two contacts are force-merged. The contact with
                    the most recent `last_modified` timestamp is retained; the
                    other is deleted. When false (default), a 4xx error is
                    returned on identifier conflict.
                getId:
                  type: boolean
                  default: false
                  description: >-
                    When true, the response returns the `id` of the surviving
                    contact after merge.
servers:
  - url: https://api.brevo.com/v3
    description: https://api.brevo.com/v3
components:
  schemas:
    ContactsPostRequestBodyContentApplicationJsonSchemaAttributes:
      oneOf:
        - type: number
          format: double
        - type: integer
        - type: string
        - type: boolean
        - type: array
          items:
            type: string
      title: ContactsPostRequestBodyContentApplicationJsonSchemaAttributes
    Contacts_createContact_Response_201:
      type: object
      properties:
        id:
          type: integer
          format: int64
          description: ID of the contact when a new contact is created
      title: Contacts_createContact_Response_201
    ContactErrorModelCode:
      type: string
      enum:
        - invalid_parameter
        - missing_parameter
        - document_not_found
        - account_in_process
        - duplicate_parameter
        - method_not_allowed
        - out_of_range
      description: Error code displayed in case of a failure
      title: ContactErrorModelCode
    ContactErrorModelMetadata:
      type: object
      properties: {}
      description: Additional information about the error
      title: ContactErrorModelMetadata
    contactErrorModel:
      type: object
      properties:
        code:
          $ref: '#/components/schemas/ContactErrorModelCode'
          description: Error code displayed in case of a failure
        message:
          type: string
          description: Readable message associated to the failure
        metadata:
          $ref: '#/components/schemas/ContactErrorModelMetadata'
          description: Additional information about the error
      required:
        - code
        - message
      title: contactErrorModel

```

**Email contact:**

```json
{
  "email": "thomas.bianchi@example.com"
}
```

**SMS contact:**

```json
{
  "attributes": {
    "SMS": "0612345678"
  }
}
```

**Email and SMS contact with attributes:**

```json
{
  "email": "thomas.bianchi@example.com",
  "attributes": {
    "SMS": "0612345678",
    "LASTNAME": "Bianchi",
    "FIRSTNAME": "Thomas",
    "DELIVERYADDRESS": "176 Boulevard des fleurs, 75014 Paris, France"
  }
}
```

**Assign to contact lists:**

```json
{
  "email": "thomas.bianchi@example.com",
  "listIds": [1, 5]
}
```

Get contact list IDs from [Contacts > Lists](https://my.brevo.com/lists) in the Brevo platform or using the [get all lists](/reference/get-lists) endpoint.

### Request example

```curl
curl --request POST \
  --url https://api.brevo.com/v3/contacts \
  --header 'accept: application/json' \
  --header 'api-key: YOUR_API_KEY' \
  --header 'content-type: application/json' \
  --data '{
    "email": "john.doe@example.com",
    "attributes": {
      "SMS": "0611223344",
      "FNAME": "John",
      "LNAME": "Doe"
    },
    "listIds": [11],
    "emailBlacklisted": false,
    "smsBlacklisted": false,
    "updateEnabled": false
  }'
```

### Response

A successful response returns status code **201** with the contact ID:

```json
{
  "id": 123
}
```

If the request fails, you receive a 400 error. Common errors include:

* Invalid email address or phone number
* Email or phone number already exists in your database
* Missing or invalid API key
* Missing `Content-Type: application/json` header

### Verify the contact

After creating a contact, verify it was created:

1. Check the [Contacts section](https://my.brevo.com/users/list) in the Brevo platform
2. Use the [retrieve contact information](/reference/get-contact-info) endpoint

Test the endpoint using the [API Reference](/reference/create-contact). Testing makes real API calls that count against your rate limits and credits.

### Code examples

```bash
curl --request POST \
  --url https://api.brevo.com/v3/contacts \
  --header 'api-key:YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{"email": "testmail@example.com", "attributes": {"SMS": "0611223344", "FNAME": "John", "LNAME": "Doe"}, "listIds": [11], "emailBlacklisted": false, "smsBlacklisted": false, "updateEnabled": false}'
```

### Manage contact lists

**Assign contacts to lists:**

```json
{
  "listIds": [1, 5]
}
```

**Unassign contacts from lists:**

```json
{
  "unlinkListIds": [1]
}
```

## Common use cases

### Create a contact with email and attributes

```json
{
  "email": "customer@example.com",
  "attributes": {
    "FNAME": "Jane",
    "LNAME": "Smith",
    "BIRTHDATE": "1990-01-15",
    "CITY": "Paris"
  },
  "listIds": [1]
}
```

### Update contact attributes

```json
{
  "attributes": {
    "CITY": "London",
    "COUNTRY": "UK"
  }
}
```

### Add contact to multiple lists

```json
{
  "listIds": [1, 2, 3]
}
```

## Troubleshooting

### Error: Invalid email address

**Problem**: Request fails with email validation error.

**Solution**: Verify the email address format is correct (e.g., `user@example.com`).

### Error: Contact already exists

**Problem**: Request fails because contact already exists.

**Solution**: Set `updateEnabled: true` in the create request to update existing contacts, or use the update endpoint instead.

### Error: Missing API key

**Problem**: Request fails with authentication error.

**Solution**: Ensure the `api-key` header is included in your request with a valid API key.

## Next steps

* Learn about [contact attributes](/reference/get-attributes)
* Manage [contact lists](/reference/get-lists)
* Import contacts using [CSV files](https://help.brevo.com/hc/en-us/articles/115000719584-Importing-your-contacts-into-SendinBlue)
* Review the [full API reference](/reference/create-contact)