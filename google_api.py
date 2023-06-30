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

    def write_note(self, sheet_name: str, cell_range: str, note: str):
        """Add a note to a cells range"""
        response = self.service_sheet.spreadsheets().values().batchUpdate(
            spreadsheetId=self.sheet_id,
            valueInputOption="USER_ENTERED",
            range=cell_range,
            body={
                "requests": [
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": sheet_name,
                                "startRowIndex": 1,
                                "endRowIndex": 1,
                                "startColumnIndex": 1,
                                "endColumnIndex": 1
                            },
                            "rows": [
                                {
                                    "values": [
                                        {
                                            "note": "my note"
                                        }
                                    ]
                                }
                            ],
                            "fields": "note"
                        }
                    }
                ]
            }
        ).execute()

        return response

    def merge_cells(self, n_row_start: int, n_row_end: int, n_col_start: int, n_col_end: int, internal_sheet_id: int):
        """
        Merge a cells range
        Careful: Indexing starts at 0 in the Google Sheet, for both rows and columns.
        """
        response = self.service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_id,
            body={
                "requests": [
                    {
                        "mergeCells": {
                            "range": {
                                "sheetId": internal_sheet_id,
                                "startRowIndex": n_row_start,
                                "endRowIndex": n_row_end,
                                "startColumnIndex": n_col_start,
                                "endColumnIndex": n_col_end
                            },
                            "mergeType": "MERGE_ALL"
                        }
                    }
                ]
            }
        ).execute()

        return response

    def unmerge_cells(self, n_row_start: int, n_row_end: int, n_col_start: int, n_col_end: int, internal_sheet_id: int):
        """
        Merge a cells range
        Careful: Indexing starts at 0 in the Google Sheet, for both rows and columns.
        """
        response = self.service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_id,
            body={
                "requests": [
                    {
                        "unmergeCells": {
                            "range": {
                                "sheetId": internal_sheet_id,
                                "startRowIndex": n_row_start,
                                "endRowIndex": n_row_end,
                                "startColumnIndex": n_col_start,
                                "endColumnIndex": n_col_end
                            }
                        }
                    }
                ]
            }
        ).execute()

        return response

    @staticmethod
    def excel_date(date1):
        """Transform a date into an Excel Integer"""
        temp = datetime(1899, 12, 30)  # Note, not 31st Dec but 30th!
        delta = date1 - temp
        out = float(delta.days) + (float(delta.seconds) / 86400)

        return out

    @staticmethod
    def n_to_col(n_col: int):
        """Get col letter from integer"""
        pass
