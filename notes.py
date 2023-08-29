import logging
import pandas as pd
from sqlalchemy import create_engine

from database_handling import DatabaseHandler
from google_api import Google


class Notes:
    def __init__(self, secrets):
        self.logger = logging.getLogger(__name__)
        
        self.secrets = secrets
        self.db_engine = create_engine(url=self.secrets["database"]["url"])
        self.dbh = DatabaseHandler(self.db_engine, secrets)
    
    def write_notes(self):
        """
        Update the notes on the pricing sheet
        """
        self.logger.info(f"The time right now is: {pd.Timestamp.now()}")
    
        sql = open("sql/task_notes.sql").read()
        bookings = self.dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})
        g = Google(secrets=self.secrets, workbook_id=self.secrets["google"]["pricing_workbook_id"])

        # Get list of flats
        flats = [f[0] for f in self.secrets["flats"].items() if f[1]["pricing_col"] != ""]
    
        # Clear workbook:
        g.write_note(0, 998, 0, 100, "", 920578163)
        g.unmerge_cells(0, 999, 0, 100, 920578163)
    
        dat = []
        merg = []
        notes = []
    
        for flat in flats:
            self.logger.info(f"Processing notes in flat {flat}")
            self.logger.info(f"Cleared worksheet of values and notes.")
    
            # Filter the bookings:
            b = bookings[bookings["object"] == flat]
    
            # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
            b.apply(self.add_write_snippet, axis=1, args=(dat, flat, g))
            b.apply(self.add_notes_snippet, axis=1, args=(notes, flat, g, 920578163))
            b.apply(self.add_merge_snippet, axis=1, args=(merg, flat, g, 920578163))
    
        # Once you are done with the workbook, execute the batchRequest:
        # Write cell values
        g.batch_write_to_cell(data=dat)
        # Write notes
        g.batch_write_notes(requests=notes)
        # Merge booking cells
        g.batch_request(requests=merg)
    
        self.logger.info("Processed all notes for this flat.")

    def add_write_snippet(self, booking, data, flat, google):
        cell_range = google.get_rolling_range(unit_id=flat, date1=booking["reservation_start"], headers_rows=3, col=self.secrets["flats"][flat]["pricing_col"])
        part1 = booking["guest_name"].split(" ")[0].title()
        try:
            part2 = booking["guest_name"].split(" ")[1][0].title() + "."
        except IndexError as ie:
            part2 = ""
        shortened_name = f"""{part1} {part2}"""
        snippet = {
            "range": cell_range,
            "values": [
                [f"""{shortened_name} ({booking["platform"][0]})"""]
            ]
        }
        data.append(snippet)

    def add_notes_snippet(self, booking, notes, flat, google, internal_sheet_id, headers_rows: int = 3):
        # Compute the ROLLING offset, based on today - 15 - headers_row:
        offset_exact = google.excel_date(booking["reservation_start"])
        offset_first = google.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
        row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

        duration = (booking["reservation_end"] - booking["reservation_start"]).days
        note_body = f"""{booking["guest_name"].title()}\nPaid {booking["total_amount_paid_by_guest"]}€\nGuests: {booking["n_guests"]}\nNights: {duration}\nFrom {booking["reservation_start"].strftime("%d.%m")} To {booking["reservation_end"].strftime("%d.%m")}\nID: {booking["booking_id"]}"""

        snippet = {
            "updateCells": {
                "range": {
                    "sheetId": internal_sheet_id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": google.col2num(self.secrets["flats"][flat]["pricing_col"]),
                    "endColumnIndex": google.col2num(self.secrets["flats"][flat]["pricing_col"]) + 1
                },
                "rows": [
                    {
                        "values": [
                            {
                                "note": note_body
                            }
                        ]
                    }
                ],
                "fields": "note"
            }
        }
        notes.append(snippet)

    def add_merge_snippet(self, booking, merg, flat, google, internal_sheet_id, headers_rows: int = 3):
        # Compute the ROLLING offset, based on today - 15 - headers_row:
        offset_exact = google.excel_date(booking["reservation_start"])
        offset_first = google.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
        row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

        snippet = {
            "mergeCells": {
                "range": {
                    "sheetId": internal_sheet_id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row + int((booking["reservation_end"] - booking["reservation_start"]).days) - 1,
                    "startColumnIndex": google.col2num(self.secrets["flats"][flat]["pricing_col"]),
                    "endColumnIndex": google.col2num(self.secrets["flats"][flat]["pricing_col"]) + 1
                },
                "mergeType": "MERGE_ALL"
            }
        }
        merg.append(snippet)
