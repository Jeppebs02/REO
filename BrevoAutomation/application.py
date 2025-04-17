import os
import zipfile
from io import BytesIO
import openpyxl

from BrevoAutomation.droprequester import DropRequester

if __name__ == '__main__':
    drop_requester = DropRequester()


    zip_data = drop_requester.download_file("/REO/Medlems-liste/Medlemmer-alle-01.01.2025.xlsx")

    with open("downloaded_file.xlsx", "wb") as f:
        f.write(zip_data)

    print("File downloaded successfully.")



    workbook = openpyxl.load_workbook(filename="downloaded_file.xlsx")
    sheet = workbook.active
    for row in sheet.iter_rows(values_only=True):
        print(row)

