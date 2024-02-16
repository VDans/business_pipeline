import logging
import pandas as pd
from sqlalchemy import create_engine

from database_handling import DatabaseHandler


class NotesH:
    def __init__(self, secrets, google, write: bool = True, merge: bool = True, notes: bool = True, color: bool = True, prices: bool = True):
        self.logger = logging.getLogger(__name__)

        self.secrets = secrets
        self.g = google
        self.write = write
        self.notes = notes
        self.color = color
        self.merge = merge
        self.prices = prices

        self.db_engine = create_engine(url=self.secrets["database"]["url"])
        self.dbh = DatabaseHandler(self.db_engine, secrets)

    def write_notes(self):
        """
        Update the notes on the pricing sheet
        """
        self.logger.info(f"The time right now is: {pd.Timestamp.now()}")

        sql = open("sql/task_notes_horizontal.sql").read()
        sql1 = open("sql/task_notes_horizontal_pricing.sql").read()
        bookings = self.dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start_adjusted": pd.Timestamp, "reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})
        pricing = self.dbh.query_data(sql=sql1, dtypes={"price": int, "price_date": pd.Timestamp, "min_nights": int, "object": str})

        # Get list of flats
        flats = [f[0] for f in self.secrets["flats"].items() if "pricing_row" in f[1]]
        bookings = bookings[bookings["object"].isin(flats)]
        pricing = pricing[pricing["object"].isin(flats)]

        # Clear workbook:
        self.g.clear_range(cell_range="C3:ZZ1000")
        self.g.write_note(0, 998, 0, 1000, "", 0)
        self.g.unmerge_cells(0, 999, 2, 1000, 0)
        self.g.uncolor_cells(0, 999, 2, 1000, 0)

        self.logger.info(f"Cleared worksheet of values and notes.")

        dat = []
        notes = []
        clr = []
        merg = []

        # Filter the bookings:
        # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
        pricing.apply(self.add_price_write_snippet, axis=1, args=(dat,))  # For prices
        pricing.apply(self.add_min_write_snippet, axis=1, args=(dat,))  # For min nights
        bookings.apply(self.add_write_snippet, axis=1, args=(dat,))  # For bookings
        bookings.apply(self.add_notes_snippet, axis=1, args=(notes, 0))
        bookings.apply(self.add_color_snippet, axis=1, args=(clr, 0))
        bookings.apply(self.add_merge_snippet, axis=1, args=(merg, 0))

        # Finally, finish with the formatting of today in yellow, vertically:
        self.add_today_color_snippet(clr=clr, internal_sheet_id=0)
        self.add_tomorrow_color_snippet(clr=clr, internal_sheet_id=0)

        # Once you are done with the workbook, execute the batchRequest:
        # Write cell values
        if self.write:
            self.g.batch_write_to_cell(data=dat)
        # Write notes
        if self.notes:
            self.g.batch_write_notes(requests=notes)
        # Color booking cells
        if self.color:
            self.g.batch_request(requests=clr)
        # Merge booking cells
        if self.merge:
            self.g.batch_request(requests=merg)

        self.logger.info("Processed all notes for this flat.")

    def add_price_write_snippet(self, pric, data):
        """
        The pricing object needs to contain the object, the price_date, and the value (price or min_night)!
        """
        if self.prices:
            target_col = self.g.get_rolling_col(date1=pric["price_date"], today_col="L")  # Column of this price_date
            snippet = {
                "range": target_col + str(self.secrets["flats"][pric["object"]]["pricing_row"]),
                "values": [
                    [f"""{pric["price"]}"""]
                ]
            }
            data.append(snippet)

    def add_min_write_snippet(self, pric, data):
        """
        The pricing object needs to contain the object, the price_date, and the value (price or min_night)!
        """
        if self.prices:
            target_col = self.g.get_rolling_col(date1=pric["price_date"], today_col="L")  # Column of this price_date
            snippet = {
                "range": target_col + str(self.secrets["flats"][pric["object"]]["pricing_row"] + 1),
                "values": [
                    [f"""{pric["min_nights"]}"""]
                ]
            }
            data.append(snippet)

    def add_write_snippet(self, booking, data):
        if self.write:
            # Calculate the A1 notation of where the name of the booking should be.
            # In this new concept, the name should expand on two rows.
            target_col = self.g.get_rolling_col(date1=booking["reservation_start_adjusted"], today_col="L")
            part1 = booking["guest_name"].split(" ")[0].title()
            part1 = part1 if (booking["reservation_end"] - booking["reservation_start_adjusted"]).days > 1 else part1[0]
            try:
                part2 = booking["guest_name"].split(" ")[1][0].title() + "."
            except IndexError:
                part2 = ""
            shortened_name = f"""{part1} {part2}"""
            snippet = {
                "range": target_col + str(self.secrets["flats"][booking["object"]]["pricing_row"]),
                "values": [
                    [f"""{shortened_name}"""]
                ]
            }
            data.append(snippet)

        else:
            pass

    def add_notes_snippet(self, booking, notes, internal_sheet_id):
        if self.notes:
            target_col = self.g.get_rolling_col(date1=booking["reservation_start_adjusted"], today_col="L")

            duration = (booking["reservation_end"] - booking["reservation_start"]).days
            note_body = f"""{booking["guest_name"].title()}\n{booking["platform"].title()}\nPaid {booking["total_amount_paid_by_guest"]}€\nGuests: {booking["n_guests"]}\nNights: {duration}\nFrom {booking["reservation_start"].strftime("%d.%m")} To {booking["reservation_end"].strftime("%d.%m")}\nID: {booking["booking_id"]}"""

            snippet = {
                "updateCells": {
                    "range": {
                        "sheetId": internal_sheet_id,
                        "startRowIndex": self.secrets["flats"][booking["object"]]["pricing_row"] - 1,
                        "endRowIndex": self.secrets["flats"][booking["object"]]["pricing_row"],
                        "startColumnIndex": self.g.col2num(target_col),
                        "endColumnIndex": self.g.col2num(target_col) + 1
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

        else:
            pass

    def add_color_snippet(self, booking, clr, internal_sheet_id):
        if self.color:
            target_col = self.g.get_rolling_col(date1=booking["reservation_start_adjusted"], today_col="L")
            if booking["platform"] == "Booking":
                r = 216
                g = 224
                b = 243
                a = 255
            elif booking["platform"] == "Airbnb":
                r = 235
                g = 206
                b = 204
                a = 255
            else:
                r = 221
                g = 233
                b = 211
                a = 255

            snippet = {
                    "repeatCell": {
                        "range": {
                            "sheetId": internal_sheet_id,
                            "startRowIndex": self.secrets["flats"][booking["object"]]["pricing_row"] - 1,
                            "endRowIndex": self.secrets["flats"][booking["object"]]["pricing_row"],  # Should be 2 rows wide.
                            "startColumnIndex": self.g.col2num(target_col),
                            "endColumnIndex": self.g.col2num(target_col) + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": r/255,
                                    "green": g/255,
                                    "blue": b/255,
                                    "alpha": a/255
                                }
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor)"
                    }
                }
            clr.append(snippet)

        else:
            pass

    def add_merge_snippet(self, booking, merg, internal_sheet_id):
        if self.merge:
            target_col = self.g.get_rolling_col(date1=booking["reservation_start_adjusted"], today_col="L")

            snippet = {
                "mergeCells": {
                    "range": {
                        "sheetId": internal_sheet_id,
                        "startRowIndex": self.secrets["flats"][booking["object"]]["pricing_row"] - 1,
                        "endRowIndex": self.secrets["flats"][booking["object"]]["pricing_row"] + 1,  # Should be 2 rows wide.
                        "startColumnIndex": self.g.col2num(target_col),
                        "endColumnIndex": self.g.col2num(target_col) + int((booking["reservation_end"] - booking["reservation_start_adjusted"]).days)
                    },
                    "mergeType": "MERGE_ALL"
                }
            }
            merg.append(snippet)

        else:
            pass

    def add_today_color_snippet(self, clr, internal_sheet_id):
        if self.color:
            target_col = "L"
            r = 244
            g = 219
            b = 102
            a = 255

            snippet = {
                    "repeatCell": {
                        "range": {
                            "sheetId": internal_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 200,  # Should be 2 rows wide.
                            "startColumnIndex": self.g.col2num(target_col),
                            "endColumnIndex": self.g.col2num(target_col) + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": r/255,
                                    "green": g/255,
                                    "blue": b/255,
                                    "alpha": a/255
                                }
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor)"
                    }
                }
            clr.append(snippet)

        else:
            pass

    def add_tomorrow_color_snippet(self, clr, internal_sheet_id):
        if self.color:
            target_col = "L"
            r = 251
            g = 242
            b = 204
            a = 255

            snippet = {
                    "repeatCell": {
                        "range": {
                            "sheetId": internal_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 200,  # Should be 2 rows wide.
                            "startColumnIndex": self.g.col2num(target_col) + 1,
                            "endColumnIndex": self.g.col2num(target_col) + 2
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": r/255,
                                    "green": g/255,
                                    "blue": b/255,
                                    "alpha": a/255
                                }
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor)"
                    }
                }
            clr.append(snippet)

        else:
            pass