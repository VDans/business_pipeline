import base64
import logging
import pandas as pd
from datetime import datetime

# from email.message import EmailMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Google:
    def __init__(self, secrets, workbook_id):
        self.secrets = secrets
        self.logger = logging.getLogger()
        self.service_account_file = "google_secrets.json"
        self.workbook_id = workbook_id  #

        self.service_sheet = self.authenticate_sheets()
        # self.service_gmail = self.authenticate_gmail()

    def authenticate_sheets(self):
        credentials = service_account.Credentials.from_service_account_file(filename=self.service_account_file)
        out = build('sheets', 'v4', credentials=credentials)
        self.logger.info("Authentication SHEETS called")
        return out

    def authenticate_gmail(self):
        credentials = service_account.Credentials.from_service_account_file(filename=self.service_account_file)
        delegated_creds=credentials.with_subject("office@host-it.at")
        out = build('gmail', 'v1', credentials=delegated_creds)
        self.logger.info("Authentication GMAIL called")
        return out

    def read_cell(self, cell_range: str):
        """Read the content of a specific cell"""
        response = self.service_sheet.spreadsheets().values().get(
            spreadsheetId=self.workbook_id,
            range=cell_range
        ).execute()
        try:
            out = response["values"][0][0]
        except KeyError:
            out = ""
        return out

    def write_to_cell(self, cell_range: str, value="Booked"):
        """Modify the content of a specific cell range"""
        response = self.service_sheet.spreadsheets().values().update(
            spreadsheetId=self.workbook_id,
            valueInputOption="USER_ENTERED",
            range=cell_range,
            body={
                "majorDimension": "ROWS",
                "values": [[value]]
            }
        ).execute()
        self.logger.info(f"Wrote {value} to cell {cell_range}")

        return response

    def clear_range(self, cell_range):
        """Clear a Google Sheet cell range"""
        response = self.service_sheet.spreadsheets().values().clear(
            spreadsheetId=self.workbook_id,
            range=cell_range
        ).execute()

        return response

    def get_pricing_range(self, unit_id: str, date1: datetime, col: str = None, offset: int = 45075):
        """This function returns the row number within the pricing sheet, on which the given date is found."""

        row = int(self.excel_date(date1) - offset)  # The first date in the pricing range is the 1st of June (45078), BUT dates only start from row 3.
        if not col:
            col = self.secrets["booking_flat_columns"][unit_id]
        sheet_range = col + str(row)

        return sheet_range

    def write_note(self, n_row_start: int, n_row_end: int, n_col_start: int, n_col_end: int, note: str, internal_sheet_id: int = 920578163):
        """Add a note to a cells range"""
        response = self.service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=self.workbook_id,
            body={
                "requests": [
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": internal_sheet_id,
                                "startRowIndex": n_row_start,
                                "endRowIndex": n_row_end,
                                "startColumnIndex": n_col_start,
                                "endColumnIndex": n_col_end
                            },
                            "rows": [
                                {
                                    "values": [
                                        {
                                            "note": note
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

    def write_note2(self, date_from: pd.Timestamp, date_to: pd.Timestamp, flat_name: str, note: str, offset=45075):
        """
        Write note in the Google Pricing Sheet from two dates
        """
        start_row_incl = self.excel_date(date_from) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        end_row_excl = self.excel_date(date_to) - offset  # Should be at least +1 compared to start row
        start_col_incl = self.secrets["booking_flat_columns_index_0"][flat_name]
        end_col_excl = start_col_incl + 1

        response = self.write_note(n_row_start=start_row_incl,
                                   n_row_end=end_row_excl,
                                   n_col_start=start_col_incl,
                                   n_col_end=end_col_excl,
                                   note=note)
        return response

    def merge_cells(self, n_row_start: int, n_row_end: int, n_col_start: int, n_col_end: int, internal_sheet_id: int = 920578163):
        """
        Merge a cells range
        Careful: Indexing starts at 0 in the Google Sheet, for both rows and columns.
        """
        response = self.service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=self.workbook_id,
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

    def merge_cells2(self, date_from: pd.Timestamp, date_to: pd.Timestamp, flat_name: str, offset=45075):
        """
        merge cells in the Google Pricing Sheet from two dates
        """
        start_row_incl = self.excel_date(date_from) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        end_row_excl = self.excel_date(date_to) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        start_col_incl = self.secrets["booking_flat_columns_index_0"][flat_name]
        end_col_excl = start_col_incl + 1

        response = self.merge_cells(n_row_start=start_row_incl,
                                    n_row_end=end_row_excl,
                                    n_col_start=start_col_incl,
                                    n_col_end=end_col_excl,
                                    internal_sheet_id=920578163)
        return response

    def unmerge_cells2(self, date_from: pd.Timestamp, date_to: pd.Timestamp, flat_name: str, offset=45075):
        """
        unmerge cells in the Google Pricing Sheet from two dates
        """
        start_row_incl = self.excel_date(date_from) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        end_row_excl = self.excel_date(date_to) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        start_col_incl = self.secrets["booking_flat_columns_index_0"][flat_name]
        end_col_excl = start_col_incl + 1

        response = self.unmerge_cells(n_row_start=start_row_incl,
                                      n_row_end=end_row_excl,
                                      n_col_start=start_col_incl,
                                      n_col_end=end_col_excl,
                                      internal_sheet_id=920578163)
        return response

    def unmerge_cells(self, n_row_start: int, n_row_end: int, n_col_start: int, n_col_end: int, internal_sheet_id: int):
        """
        Merge a cells range
        Careful: Indexing starts at 0 in the Google Sheet, for both rows and columns.
        """
        response = self.service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=self.workbook_id,
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

    # def send_gmail(self, to_email: str, body: str, subject=""):
    #     message = EmailMessage()
    #     message.set_content(body)
    #     message["To"] = to_email
    #     message["From"] = "office@host-it.at"
    #     message["Subject"] = subject
    #     encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    #     create_message = {
    #         'raw': encoded_message
    #     }
    #
    #     try:
    #         self.service_gmail.users().messages().send(userId="me", body=create_message).execute()
    #     except HttpError as error:
    #         logging.error(f'{error}')

    @staticmethod
    def excel_date(date1):
        """Transform a date into an Excel Integer"""
        temp = datetime(1899, 12, 30)  # Note, not 31st Dec but 30th!
        delta = date1 - temp
        out = float(delta.days) + (float(delta.seconds) / 86400)

        return int(out)

    @staticmethod
    def n_to_col(n_col: int):
        """Get col letter from integer"""
        pass
