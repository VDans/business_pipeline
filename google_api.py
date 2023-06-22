import logging
import requests
import pandas as pd
from datetime import datetime, timedelta


class GoogleSheetsAPI:
    def __init__(self):
        # Here the auth credentials
        self.url = "https://sheets.googleapis.com/v4/spreadsheets"
        self.token = "ya29.a0AWY7CknvZO2DtyUZRtxfTBaGiVkAi1EP_BkfOQ8NviXxQg_3gl01zax5tulLOZtqNCWMxooz8xhq9EvwCeIhJfZeL-glyza7wKbY8xp1T3KjvlPuuJF_bHKVVqlS5B7xf-mpWAO5ZbM2cFYxwM-P26ciEPEkOkcaCgYKAXESARISFQG1tDrpcnpJ5G3ZI2T2gbDIVxx5fg0166"
        self.headers = {"Authorization": "Bearer " + self.token,
                        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly",
                        "ConsistencyLevel": "eventual"}
        self.logger = logging.getLogger()
        self.sheet_id = "17c7HeZQtNGJgTaE6xUaSYFI1wkPqvnxwzLqFLXRNXps"

    def get_range(self, cells_range):
        """Get the values from a given range"""
        response = requests.request(headers=self.headers,
                                    method="GET",
                                    url=f"{self.url}/{self.sheet_id}/values:batchGet",
                                    params={
                                        "ranges": cells_range,
                                        "majorDimension": "COLUMNS",
                                        "valueRenderOption": "UNFORMATTED_VALUE"
                                    })
        return response

    @staticmethod
    def from_excel_ordinal(ordinal: int, _epoch0=datetime(1899, 12, 31)) -> datetime:
        if ordinal >= 60:
            ordinal -= 1  # Excel leap year bug, 1900 is not a leap year!
        return (_epoch0 + timedelta(days=ordinal)).replace(microsecond=0)


g = GoogleSheetsAPI()
response1 = g.get_range(cells_range="A:C").json()
df = pd.DataFrame({'date': response1["valueRanges"][0]["values"][0], 'min_nights': response1["valueRanges"][0]["values"][1], 'price': response1["valueRanges"][0]["values"][2]})
print(df)
