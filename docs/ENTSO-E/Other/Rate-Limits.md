## Rate Limits and Usage Restrictions

### Applied Rate Limits

* **Primary Limit:** 400 requests per minute per user account (API token).
* **Enforcement Scope:** Limits are applied per user account (API token), not per IP address.

### IP Banning Policy

* **No IP-Based Banning in TP R3 API:** With the transition to the TP R3 API, rate limiting and bans are no longer applied based on IP addresses.
* Usage is tracked and restricted solely on a per-user-account (API token) basis.

### Circumstances Leading to Temporary Bans

#### Exceeding Rate Limits

If a user account exceeds 400 requests within a one-minute window, the associated API token may be temporarily banned.

#### Automatic Unbanning

Temporary bans are automatically lifted after approximately 10 minutes.

#### Distributed Systems

When multiple services, servers, containers, or Kubernetes nodes share the same API token, all requests are aggregated under that token. Even if requests originate from different IP addresses, the combined request volume counts toward the same 400 requests-per-minute limit and may trigger a temporary ban.

#### Token Misuse

If an API token is suspected to be compromised, abused, or used in violation of ENTSO-E policies, ENTSO-E reserves the right to revoke the token.

### Recommendations

#### Monitor Aggregate Request Rates

Track the combined request volume across all applications and infrastructure components using the same API token.

#### Implement Client-Side Throttling

Implement request throttling and rate limiting in client applications. A sustained rate of approximately **6–7 requests per second** provides a safe margin below the published limit while allowing for occasional bursts.

#### Regenerate Compromised Tokens

If unusual activity is detected or a token is believed to be exposed, generate a new API token and retire the old one.

#### Information to Include When Reporting Issues

When contacting ENTSO-E support regarding rate limiting or access issues, provide:

* User account email associated with the API token.
* Exact timestamps of any HTTP 429 responses.
* Request counts per minute for the affected token.
* Relevant application logs showing request activity.
