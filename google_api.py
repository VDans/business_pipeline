import logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build


class Google:
    def __init__(self, secrets):
        self.secrets = secrets
        self.logger = logging.getLogger()
        self.service_account_file = "google_secrets.json"
        self.sheet_id = "17c7HeZQtNGJgTaE6xUaSYFI1wkPqvnxwzLqFLXRNXps"

        self.service_sheet = self.authenticate()

    def authenticate(self):
        credentials = service_account.Credentials.from_service_account_file(filename=self.service_account_file)
        out = build('sheets', 'v4', credentials=credentials)
        self.logger.info("Authentication called")
        return out

    def write_to_cell(self, cell_range: str, value="Booked"):
        """Modify the content of a specific cell range"""
        response = self.service_sheet.spreadsheets().values().update(
            spreadsheetId=self.sheet_id,
            valueInputOption="USER_ENTERED",
            range=cell_range,
            body={
                "majorDimension": "ROWS",
                "values": [[value]]
            }
        ).execute()
        self.logger.info(f"Wrote {value} to cell {cell_range}")

        return response

    def get_pricing_range(self, unit_id: str, date1: datetime, offset=45075):
        """This function returns the row number within the pricing sheet, on which the given date is found."""

        row = int(self.excel_date(date1) - offset)  # The first date in the pricing range is the 1st of June (45078), BUT dates only start from row 3.
        col = self.secrets["booking_flat_columns"][unit_id]
        sheet_range = col + str(row)

        return sheet_range

    @staticmethod
    def excel_date(date1):
        """Transform a date into an Excel Integer"""
        temp = datetime(1899, 12, 30)  # Note, not 31st Dec but 30th!
        delta = date1 - temp
        out = float(delta.days) + (float(delta.seconds) / 86400)

        return out
