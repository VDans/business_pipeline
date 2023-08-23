import json

import pandas as pd

from google_api import Google
from message_scrapers.airbnb_message import AirbnbCom

secrets = json.load(open('config_secrets.json'))

# a = AirbnbCom(secrets)
# a.send_message(thread_id="1571685347", image_path="C:/Users/vdans/PycharmProjects/business_pipeline/Resources/CZERNIN/step1.jpg")
SHEET_ID = 920578163

g = Google(secrets, '17c7HeZQtNGJgTaE6xUaSYFI1wkPqvnxwzLqFLXRNXps')


r = [
    {
        "moveDimension": {
            "source": {
                "sheetId": SHEET_ID,
                "dimension": "ROWS",
                "startIndex": 4,
                "endIndex": 459
            },
            "destinationIndex": 3
        }
    }
  ]

g.batch_request(requests=r)

# def batch_request(self, requests: list):
#     """Request many things at once"""
#     response = self.service_sheet.spreadsheets().batchUpdate(
#         spreadsheetId=self.workbook_id,
#         body={
#             "requests": requests
#         }
#     ).execute()
#
#     return response
