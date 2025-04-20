from __future__ import print_function
import os

import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

import requests
import time


class BrevoRequester:
    def __init__(self):
        self.brevo_api_key = os.getenv("BREVO_API_KEY")
        if not self.brevo_api_key:
            raise ValueError(f"{self.brevo_api_key} environment variable not set")
        self.base_url = "https://api.brevo.com/v3"
        self.headers = {
            "Content-Type": "application/json",
            "api-key": self.brevo_api_key
        }
    def _make_request(self, method, endpoint, data=None):
        """Internal helper for making requests and basic error handling."""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.request(method, url, headers=self.headers, json=data)
            # Check for common Brevo rate limiting
            if response.status_code == 429:
                 print("Warning: Brevo rate limit hit. Consider adding delays.")
                 # add a retry mechanism here with time.sleep()
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            # Handle cases where Brevo might return empty body on success (e.g., 204 No Content)
            if response.status_code == 204:
                return {"success": True, "status_code": 204}
            return response.json()
        except requests.exceptions.RequestException as e:
            error_details = "No response body or non-JSON error."
            status_code = "N/A"
            if e.response is not None:
                status_code = e.response.status_code
                try:
                     error_details = e.response.json()
                except requests.exceptions.JSONDecodeError:
                     error_details = e.response.text
            print(f"Brevo API Request Error: {method} {url} failed. Status: {status_code}. Details: {error_details}. Error: {e}")
            # Return a consistent error format
            return {"error": True, "status_code": status_code, "message": str(e), "details": error_details}
        except Exception as e: # Catch other potential errors
             print(f"Unexpected error during Brevo API request: {e}")
             return {"error": True, "status_code": "N/A", "message": str(e)}


    def create_update_contact(self, contact_data):
        """
        Creates a new contact or updates an existing one based on email.
        https://developers.brevo.com/reference/createcontact

        Args:
            contact_data (dict): Dictionary matching Brevo's expected format
                                 (email, attributes, listIds, updateEnabled).
        Returns:
            dict: The API response JSON or an error dictionary.
        """
        endpoint = "contacts"
        # Ensure updateEnabled is set, default to True if missing
        if 'updateEnabled' not in contact_data:
            contact_data['updateEnabled'] = True
        return self._make_request("POST", endpoint, data=contact_data)



    def update_all_members_contact_list(self, contacts_to_process, target_list_id=4):
        """
        Iterates through a list of prepared contact data and attempts to
        create or update each contact in Brevo, adding them to the target list.

        Args:
            contacts_to_process (list): A list of contact dictionaries,
                                        prepared by prepare_brevo_contacts_from_excel.
            target_list_id (int): The Brevo list ID to ensure contacts are added to.
        """
        if not contacts_to_process:
            print("No contacts provided to update.")
            return

        print(f"\n--- Starting Brevo Contact Update for List ID {target_list_id} ---")
        success_count = 0
        failure_count = 0

        for i, contact_data in enumerate(contacts_to_process):
            email = contact_data.get("email", "N/A")
            print(f"Processing contact {i+1}/{len(contacts_to_process)}: {email}...")

            # Ensure the target list ID is in the contact's listIds
            if target_list_id not in contact_data.get("listIds", []):
                if "listIds" not in contact_data:
                    contact_data["listIds"] = []
                contact_data["listIds"].append(target_list_id)
                # Ensure uniqueness if needed, though Brevo likely handles it
                contact_data["listIds"] = list(set(contact_data["listIds"]))

            response = self.create_update_contact(contact_data)

            # Check response for success/failure
            if isinstance(response, dict) and response.get("error"):
                 failure_count += 1
                 print(f"  -> Failed for {email}. Status: {response.get('status_code')}. Reason: {response.get('details', response.get('message'))}")
            elif isinstance(response, dict) and (response.get("id") or response.get("success")): # Check for contact ID on creation or simple success on update/204
                 success_count += 1
                 # print(f"  -> Success for {email}.") # Less verbose
            else:
                 # Unexpected response format
                 failure_count += 1
                 print(f"  -> Failed for {email}. Unexpected Response: {response}")

            # Optional: Add a small delay to avoid rate limiting if processing many contacts. Probably not needed though.
            # time.sleep(0.1) # Sleep for 100ms between requests

        print("--- Brevo Contact Update Finished ---")
        print(f"Successfully processed: {success_count}")
        print(f"Failed: {failure_count}")
        print("-" * 35)


    def get_contact_lists(self):
        """Gets the first 10 contact lists."""
        # Keep limit low unless we specifically need more
        endpoint = "contacts/lists?limit=10&offset=0&sort=desc"
        return self._make_request("GET", endpoint)