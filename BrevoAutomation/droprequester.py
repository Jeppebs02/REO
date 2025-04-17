import requests
import os


class DropRequester:

    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        if self.api_key is None:
            raise ValueError("API_KEY environment variable not set")

        # Get api secret
        self.api_secret = os.getenv("API_SECRET")
        if self.api_secret is None:
            raise ValueError("API_SECRET environment variable not set")

        self.base_url = "https://api.dropboxapi.com/2/"

        self.OAUTH_TOKEN = os.getenv("OAUTH_TOKEN")
        if self.OAUTH_TOKEN is None:
            raise ValueError("OAUTH_TOKEN environment variable not set")

    def get_headers(self):
        return {
            "Authorization": f"Bearer {self.OAUTH_TOKEN}",
            "Content-Type": "application/json"
        }

    def get_download_headers(self, file_path):
        return {
            "Authorization": f"Bearer {self.OAUTH_TOKEN}",
            "Dropbox-API-Arg": f'{{"path": "{file_path}"}}'
        }


    def get_account_info(self):
        url = f"{self.base_url}users/get_current_account"
        headers = {"Authorization": f"Bearer {self.OAUTH_TOKEN}"}
        #data = {"query": "foo"}
        #response = requests.post(url, headers=headers, json=data)
        response = requests.post(url, headers=headers, data="")

        if response.headers.get("Content-Type") == "application/json":
            return response.json()
        else:
            return {"error": "Unexpected content", "content": response.text}



    def get_file_by_id(self, file_id):
        url = f"{self.base_url}file_requests/get"
        headers = self.get_headers()

        data = f'{{"id":"{file_id}"}}'
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": "File not found", "status_code": response.status_code}

    def list_account_files(self, limit=1000):
        url = f"{self.base_url}file_requests/list_v2"
        headers = self.get_headers()

        data = {"limit": limit}
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": "Unable to list files",
                "status_code": response.status_code,
                "details": response.text
            }

    def list_shared_links(self):
        url = f"{self.base_url}sharing/list_shared_links"
        headers = self.get_headers()

        data = {
            "path": "/REO", #Root
        }
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": "Unable to list shared files",
                "status_code": response.status_code,
                "details": response.text
            }


    def list_shared_folders(self):
        url = f"{self.base_url}files/list_folder"
        headers = self.get_headers()

        data = {
            "recursive": False, #No recursive
            "path": "/REO", #Root
        }
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": "Unable to list shared folders",
                "status_code": response.status_code,
                "details": response.text
            }


    def download_file(self, file_path):
        url = "https://content.dropboxapi.com/2/files/download"
        headers = self.get_download_headers(file_path)

        response = requests.post(url, headers=headers)

        if response.status_code == 200:
            return response.content
        else:
            return {
                "error": "Unable to download file",
                "status_code": response.status_code,
                "details": response.text
            }