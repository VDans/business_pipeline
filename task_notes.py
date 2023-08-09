import logging
import json
import time
import string
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from google_api import Google

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def write_notes():
    """
    Used when you need to correct the notes on the pricing sheet
    """
    sql = open("sql/task_notes.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start": pd.Timestamp})
    g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])
    quota = 0

    # Get list of flats
    flats = list(bookings["object"].unique())

    for flat in flats:
        logging.info(f"Processing notes in flat {flat}")
        notes = []

        # Clear workbook:
        # response1 = g.write_note(0, 998, 0, 24, "", 0)
        # logging.info(f"Cleared worksheet of values and notes.")

        # Filter the bookings:
        b = bookings[bookings["object"] == flat]

        # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
        b.apply(add_notes_snippet, axis=1, args=(notes, flat, g))

        # Once you are done with the workbook, execute the batchRequest:
        g.batch_write_notes(requests=notes)
        quota += 1
        logging.info(f"QUOTA: {quota}")

    logging.info("Processed all notes for this flat.")


def add_notes_snippet(booking, notes, flat, google, offset=45106):
    note_body = f"""{booking["guest_name"].title()}\nPaid {booking["total_amount_paid_by_guest"]}€\nGuests: {booking["n_guests"]}"""

    snippet = {
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": google.excel_date(booking["reservation_start"]) - offset - 1,
                "endRowIndex": google.excel_date(booking["reservation_start"]) - offset,
                "startColumnIndex": google.col2num(secrets["flats"][flat]["cleaning_col"]),
                "endColumnIndex": google.col2num(secrets["flats"][flat]["cleaning_col"]) + 1
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


write_cleanings()
