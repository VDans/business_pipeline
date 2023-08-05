import base64
import logging
import string
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

    def authenticate_sheets(self):
        credentials = service_account.Credentials.from_service_account_file(filename=self.service_account_file)
        out = build('sheets', 'v4', credentials=credentials)
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

    def batch_read_cells(self, ranges):
        """Read the content of a range of cells (More than 1)"""
        response = self.service_sheet.spreadsheets().values().batchGet(
            spreadsheetId=self.workbook_id,
            ranges=ranges,
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        out = response["valueRanges"]
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

    def batch_write_to_cell(self, data):
        response = self.service_sheet.spreadsheets().values().batchUpdate(
            spreadsheetId=self.workbook_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": data
            }
        ).execute()

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
            col = self.secrets["flats"][unit_id]["pricing_col"]
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

    def write_note2(self, date_from: pd.Timestamp, date_to: pd.Timestamp, flat_name: str, note: str, offset=45075, internal_sheet_id: int = 920578163, goal: str = "pricing"):
        """
        Write note in the Google Pricing Sheet from two dates
        """
        start_row_incl = self.excel_date(date_from) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        end_row_excl = self.excel_date(date_to) - offset  # Should be at least +1 compared to start row
        start_col_incl = self.col2num(self.secrets["flats"][flat_name]["pricing_col"]) if goal == 'pricing' else self.col2num(self.secrets["flats"][flat_name]["cleaning_col"])
        end_col_excl = start_col_incl + 1

        response = self.write_note(n_row_start=start_row_incl,
                                   n_row_end=end_row_excl,
                                   n_col_start=start_col_incl,
                                   n_col_end=end_col_excl,
                                   note=note,
                                   internal_sheet_id=internal_sheet_id)
        self.logger.info(f"Note response: {response}")
        return response

    def batch_write_notes(self, requests: list):
        """Add many notes at once"""
        response = self.service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=self.workbook_id,
            body={
                "requests": requests
            }
        ).execute()

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

    def merge_cells2(self, date_from: pd.Timestamp, date_to: pd.Timestamp, flat_name: str, offset=45075, internal_sheet_id: int = 920578163):
        """
        merge cells in the Google Pricing Sheet from two dates
        """
        start_row_incl = self.excel_date(date_from) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        end_row_excl = self.excel_date(date_to) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        start_col_incl = self.col2num(self.secrets["flats"][flat_name]["pricing_col"])
        end_col_excl = start_col_incl + 1

        response = self.merge_cells(n_row_start=start_row_incl,
                                    n_row_end=end_row_excl,
                                    n_col_start=start_col_incl,
                                    n_col_end=end_col_excl,
                                    internal_sheet_id=internal_sheet_id)
        self.logger.info(f"Merge response: {response}")

        return response

    def unmerge_cells2(self, date_from: pd.Timestamp, date_to: pd.Timestamp, flat_name: str, offset=45075):
        """
        unmerge cells in the Google Pricing Sheet from two dates
        """
        start_row_incl = self.excel_date(date_from) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        end_row_excl = self.excel_date(date_to) - offset - 1  # -1, because indexing starts at 0 for the formatting!
        start_col_incl = self.col2num(self.secrets["flats"][flat_name]["pricing_col"])
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

    @staticmethod
    def excel_date(date1):
        """Transform a date into an Excel Integer"""
        temp = datetime(1899, 12, 30)  # Note, not 31st Dec but 30th!
        delta = date1 - temp
        out = float(delta.days) + (float(delta.seconds) / 86400)

        return int(out)

    @staticmethod
    def n_to_col(n_col: int, start_index: int = 0):
        """Get col letter from integer"""
        letter = ''
        while n_col > 25 + start_index:
            letter += chr(65 + int((n_col - start_index) / 26) - 1)
            n_col = n_col - (int((n_col - start_index) / 26)) * 26
        letter += chr(65 - start_index + (int(n_col)))
        return letter

    @staticmethod
    def col2num(col):
        num = 0
        for c in col:
            if c in string.ascii_letters:
                num = num * 26 + (ord(c.upper()) - ord('A')) + 1
        return num - 1  # Google Sheets column indexing starts at 0.
