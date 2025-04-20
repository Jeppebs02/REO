import requests
import os
import base64 # Needed for Basic Auth formatting if not using requests' auth tuple

class DropRequester:

    # APP_KEY is the Client ID
    # APP_SECRET is the Client Secret
    # REFRESH_TOKEN is the long-lived token obtained from the initial auth flow
    APP_KEY_ENV = "DROPBOX_APP_KEY"
    APP_SECRET_ENV = "DROPBOX_APP_SECRET"
    REFRESH_TOKEN_ENV = "DROPBOX_REFRESH_TOKEN" # Store THIS securely

    TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
    BASE_API_URL = "https://api.dropboxapi.com/2/"
    BASE_CONTENT_URL = "https://content.dropboxapi.com/2/"

    def __init__(self):
        self.app_key = os.getenv(self.APP_KEY_ENV)
        if not self.app_key:
            raise ValueError(f"{self.APP_KEY_ENV} environment variable not set")

        self.app_secret = os.getenv(self.APP_SECRET_ENV)
        if not self.app_secret:
            raise ValueError(f"{self.APP_SECRET_ENV} environment variable not set")

        self.refresh_token = os.getenv(self.REFRESH_TOKEN_ENV)
        if not self.refresh_token:
            raise ValueError(f"{self.REFRESH_TOKEN_ENV} environment variable not set. "
                             "You must obtain this via the initial OAuth2 authorization code flow.")

        self.access_token = None # Will be populated by _refresh_access_token
        self._refresh_access_token() # Get the initial access token upon instantiation


    def _refresh_access_token(self):
        """
        Uses the refresh token to obtain a new short-lived access token.
        """
        print("Attempting to refresh Dropbox access token...")
        try:
            response = requests.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                },
                # Basic Authentication: username=APP_KEY, password=APP_SECRET
                auth=(self.app_key, self.app_secret)
            )

            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)

            token_data = response.json()
            self.access_token = token_data.get("access_token")

            if not self.access_token:
                raise ValueError("Failed to get access_token from refresh response.")

            print("Successfully refreshed access token.")

        except requests.exceptions.RequestException as e:
            print(f"Error refreshing Dropbox token: {e}")
            # Attempt to get more details from response if available
            error_details = "No response body."
            if e.response is not None:
                try:
                    error_details = e.response.json()
                except requests.exceptions.JSONDecodeError:
                    error_details = e.response.text
            raise RuntimeError(f"Could not refresh Dropbox access token. Status: {e.response.status_code if e.response is not None else 'N/A'}. Details: {error_details}") from e
        except (ValueError, KeyError) as e:
             print(f"Error parsing token refresh response: {e}")
             raise RuntimeError(f"Could not parse token refresh response: {e}") from e


    def _get_auth_header(self):
        """Returns the Authorization header using the current access token."""
        if not self.access_token:
             # This shouldn't happen if __init__ succeeded, but good practice
             raise RuntimeError("Access token is not available. Refresh might have failed.")
        return {"Authorization": f"Bearer {self.access_token}"}



    def get_headers(self):
        # Standard headers for JSON API endpoints
        headers = self._get_auth_header()
        headers["Content-Type"] = "application/json"
        return headers

    def get_download_headers(self, file_path):
        # Specific headers for content download endpoints
        headers = self._get_auth_header()

        import json
        api_arg = json.dumps({"path": file_path})
        headers["Dropbox-API-Arg"] = api_arg

        # Let requests handle the Content-Type for the *request body* (which is empty here).
        return headers


    def get_account_info(self):
        url = f"{self.BASE_API_URL}users/get_current_account"
        headers = self._get_auth_header() # We only need auth header
        # Sending an empty POST body for this specific endpoint
        response = requests.post(url, headers=headers, data=None) # Use None or empty bytes for no body

        # Check response status code *before* assuming JSON
        if response.status_code == 200:
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError:
                 return {"error": "Received non-JSON response", "status_code": response.status_code, "content": response.text}
        else:
             # Try to return error details if possible
             try:
                  details = response.json()
             except requests.exceptions.JSONDecodeError:
                  details = response.text
             return {"error": "Failed to get account info", "status_code": response.status_code, "details": details}



    def list_shared_folders(self):
        url = f"{self.BASE_API_URL}files/list_folder"
        headers = self.get_headers() # Use the helper for JSON API headers

        data = {
            "recursive": False,
            "path": "/REO", # This is the folder/path we want to list
        }
        response = requests.post(url, headers=headers, json=data) # Send data as JSON

        if response.status_code == 200:
             try:
                 return response.json()
             except requests.exceptions.JSONDecodeError:
                  return {"error": "Received non-JSON response", "status_code": response.status_code, "content": response.text}
        else:
            try:
                details = response.json()
            except requests.exceptions.JSONDecodeError:
                details = response.text
            return {
                "error": "Unable to list shared folders",
                "status_code": response.status_code,
                "details": details
            }


    def download_file(self, file_path):
        """Downloads a file from the specified Dropbox path."""
        url = f"{self.BASE_CONTENT_URL}files/download"
        headers = self.get_download_headers(file_path)

        try:
            # Use stream=True for potentially large files. Not strictly necessary
            # if response.content fits in memory.
            response = requests.post(url, headers=headers, stream=True)
            response.raise_for_status() # Check for HTTP errors

            # Return the raw byte content directly on success
            return response.content

        except requests.exceptions.RequestException as e:
            print(f"Error downloading file from Dropbox: {e}")
            status_code = e.response.status_code if e.response is not None else 'N/A'
            details = "No response body or non-text response."
            # Try to get error details from API response
            if e.response is not None:
                try:
                    api_error_header = e.response.headers.get('Dropbox-API-Result')
                    if api_error_header:
                        details = api_error_header
                    else:
                        # Try decoding body as JSON, if we can't, we fall back to text
                         try:
                             details = e.response.json()
                         except requests.exceptions.JSONDecodeError:
                             # Read chunks in case it's large binary data + error
                             details = e.response.text[:500] # Limit text output to first 500 chars
                except Exception as inner_e:
                    details = f"Could not parse error details: {inner_e}"

            # Return the error dictionary structure our calling code expects
            return {
                "error": "Unable to download file",
                "status_code": status_code,
                "details": details
            }

    #We likely won't ever need this, as we already have the refresh token
    @staticmethod
    def get_initial_refresh_token(app_key, app_secret):
        """Helper to guide through getting the first refresh token."""
        from urllib.parse import urlencode

        # 1. Generate the Authorization URL
        auth_url_base = "https://www.dropbox.com/oauth2/authorize"
        params = {
            "client_id": app_key,
            "response_type": "code",        # Requesting an authorization code
            "token_access_type": "offline", # Request a refresh token with offline access
            # Add scopes needed by the app, space-separated if multiple
            "scope": "files.content.read files.metadata.read",
            # Optional: Add redirect_uri configured in the App Console.
            # "redirect_uri": "http://localhost:8080",
            # Optional: PKCE parameters for added security (recommended for public clients)
            # "code_challenge": "...",
            # "code_challenge_method": "S256"
        }
        auth_url = f"{auth_url_base}?{urlencode(params)}"

        print("--- Step 1: Authorize your Application ---")
        print("Visit this URL in your browser and grant access:")
        print(auth_url)
        print("-" * 40)

        # 2. Get the Authorization Code from the redirect
        auth_code = input("After authorizing, you'll be redirected (or shown a code).\n"
                          "Paste the 'code' parameter value here: ").strip()

        if not auth_code:
            print("Authorization code is needed.")
            return

        # 3. Exchange the Authorization Code for Tokens
        print("\n--- Step 2: Exchanging code for tokens ---")
        token_url = DropRequester.TOKEN_URL
        try:
            response = requests.post(
                token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": auth_code,
                    # If using PKCE, add "code_verifier": "..."
                    # If using redirect_uri, add "redirect_uri": "YOUR_REDIRECT_URI"
                },
                auth=(app_key, app_secret) # Basic Auth
            )
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in")
            scope = token_data.get("scope") # Granted scopes

            if not refresh_token:
                print("\nERROR: No refresh token received!")
                print("Ensure you requested 'token_access_type=offline'.")
                print("Response:", token_data)
                return

            print("\n--- SUCCESS! ---")
            print(f"Access Token (short-lived): {access_token}")
            print(f"Expires In (seconds): {expires_in}")
            print(f"Granted Scopes: {scope}")
            print("\n>>> REFRESH TOKEN (long-lived - STORE THIS SECURELY!) <<<")
            print(refresh_token)
            print("\nSet this Refresh Token as the environment variable: "
                  f"{DropRequester.REFRESH_TOKEN_ENV}")
            print("Also ensure your App Key and App Secret are set as env vars:")
            print(f"- {DropRequester.APP_KEY_ENV}")
            print(f"- {DropRequester.APP_SECRET_ENV}")

        except requests.exceptions.RequestException as e:
            print(f"\nError exchanging code for token: {e}")
            if e.response is not None:
                print("Status Code:", e.response.status_code)
                try:
                    print("Response Body:", e.response.json())
                except:
                     print("Response Body:", e.response.text)

