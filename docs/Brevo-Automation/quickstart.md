# Quickstart

### Get your API key

Generate an API key from your Brevo account. You'll need it to authenticate every request — copy it immediately, it's only shown once.

Open API key settings →

In the **API keys** tab, click **Generate a new API key**, name it, and save it somewhere secure.

### Whitelist your IP address

The playground runs from your browser, so Brevo needs to recognize your current IP before processing requests from it.

Your current IP address is:

Add it to the **Authorized IPs** list in your Brevo security settings.

Open IP security settings →

Prefer to skip this step? Send your first request anyway. If your IP isn't whitelisted, Brevo sends a security email with a one-click authorization link — then come back and retry.

Not required when calling the API from a server or CLI.

### Send your first request

Enter your API key in the `api-key` header in the panel on the right, then click **Send request**.

A `200` response with your account details confirms your key is valid — you're ready to build.

### Request

GET [https://api.brevo.com/v3/account](https://api.brevo.com/v3/account)

```curl response
curl https://api.brevo.com/v3/account \
     -H "api-key: <apiKey>"
```

```typescript response
import { BrevoClient } from "@getbrevo/brevo";

async function main() {
    const client = new BrevoClient({
        apiKey: "YOUR_API_KEY_HERE",
    });
    await client.account.getAccount();
}
main();

```

```python response
from brevo import Brevo

client = Brevo(
    api_key="YOUR_API_KEY_HERE",
)

client.account.get_account()

```

```php response
<?php

namespace Example;

use Brevo\Brevo;

$client = new Brevo(
    apiKey: 'YOUR_API_KEY_HERE',
);
$client->account->getAccount();

```

```go response
package main

import (
	"fmt"
	"net/http"
	"io"
)

func main() {

	url := "https://api.brevo.com/v3/account"

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("api-key", "<apiKey>")

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

url = URI("https://api.brevo.com/v3/account")

http = Net::HTTP.new(url.host, url.port)
http.use_ssl = true

request = Net::HTTP::Get.new(url)
request["api-key"] = '<apiKey>'

response = http.request(request)
puts response.read_body
```

```java response
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

HttpResponse<String> response = Unirest.get("https://api.brevo.com/v3/account")
  .header("api-key", "<apiKey>")
  .asString();
```

```csharp response
using RestSharp;

var client = new RestClient("https://api.brevo.com/v3/account");
var request = new RestRequest(Method.GET);
request.AddHeader("api-key", "<apiKey>");
IRestResponse response = client.Execute(request);
```

```swift response
import Foundation

let headers = ["api-key": "<apiKey>"]

let request = NSMutableURLRequest(url: NSURL(string: "https://api.brevo.com/v3/account")! as URL,
                                        cachePolicy: .useProtocolCachePolicy,
                                    timeoutInterval: 10.0)
request.httpMethod = "GET"
request.allHTTPHeaderFields = headers

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

### Request

GET [https://api.brevo.com/v3/account](https://api.brevo.com/v3/account)

```curl response
curl https://api.brevo.com/v3/account \
     -H "api-key: <apiKey>"
```

```typescript response
import { BrevoClient } from "@getbrevo/brevo";

async function main() {
    const client = new BrevoClient({
        apiKey: "YOUR_API_KEY_HERE",
    });
    await client.account.getAccount();
}
main();

```

```python response
from brevo import Brevo

client = Brevo(
    api_key="YOUR_API_KEY_HERE",
)

client.account.get_account()

```

```php response
<?php

namespace Example;

use Brevo\Brevo;

$client = new Brevo(
    apiKey: 'YOUR_API_KEY_HERE',
);
$client->account->getAccount();

```

```go response
package main

import (
	"fmt"
	"net/http"
	"io"
)

func main() {

	url := "https://api.brevo.com/v3/account"

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("api-key", "<apiKey>")

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

url = URI("https://api.brevo.com/v3/account")

http = Net::HTTP.new(url.host, url.port)
http.use_ssl = true

request = Net::HTTP::Get.new(url)
request["api-key"] = '<apiKey>'

response = http.request(request)
puts response.read_body
```

```java response
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

HttpResponse<String> response = Unirest.get("https://api.brevo.com/v3/account")
  .header("api-key", "<apiKey>")
  .asString();
```

```csharp response
using RestSharp;

var client = new RestClient("https://api.brevo.com/v3/account");
var request = new RestRequest(Method.GET);
request.AddHeader("api-key", "<apiKey>");
IRestResponse response = client.Execute(request);
```

```swift response
import Foundation

let headers = ["api-key": "<apiKey>"]

let request = NSMutableURLRequest(url: NSURL(string: "https://api.brevo.com/v3/account")! as URL,
                                        cachePolicy: .useProtocolCachePolicy,
                                    timeoutInterval: 10.0)
request.httpMethod = "GET"
request.allHTTPHeaderFields = headers

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

## What's next

Explore our guides to start building your integration:

* **[Learn key Brevo API concepts](/docs/how-it-works)**: Authentication, pagination, and rate limits.
* **[Send transactional emails](/docs/send-a-transactional-email)**: Send your first email and learn about template management.
* **[Setup webhooks](/docs/how-to-use-webhooks)**: Receive real-time updates for email events.
* **[Send SMS & WhatsApp](/docs/whatsapp-messages)**: Reach customers on their preferred mobile channels.

## Tools and Resources

Explore, test, and contribute to our API collections. Fork our workspace to start with pre-configured endpoints.

Rate limiting policies and quotas. Learn how to optimize API calls and prevent throttling.

Integrate Brevo with AI assistants using the Model Context Protocol. Enable AI models to interact with your Brevo account through standardized endpoints.