import logging
import json
import time
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from google_api import Google

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def write_cleanings():
    """
    This task runs once a day.

    Get flat name, checkout date, and number of guests from each reservation.
    For each, find out the correct Google workbook id and write to the correct cell according to the flat and date.

    This should be done using batchRequests!

    """
    sql = open("sql/task_cleanings.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start": pd.Timestamp})
    quota = 0

    flats = [f[0] for f in secrets["flats"].items() if f[1]["pid_booking"] != ""]
    cleaning_sheets = list(set([secrets["flats"][f]["cleaning_workbook_id"] for f in flats if secrets["flats"][f]["cleaning_workbook_id"] != ""]))

    logging.warning(f"The time right now is: {pd.Timestamp.now()}")

    for cs in cleaning_sheets:
        dat = []
        notes = []

        # Find the corresponding Google sheet for the specific cleaner:
        g = Google(secrets=secrets, workbook_id=cs)

        # Clear workbook:
        response = g.clear_range(cell_range="B2:Z1000")
        # Clear notes: Make a large batch with notes to "":
        response1 = g.write_note(0, 900, 0, 50, "", 0)
        logging.warning(f"Cleared worksheet of values and notes.")

        # Find all flats on this workbook:
        cs_flats = [f for f in secrets['flats'] if secrets["flats"][f]["cleaning_workbook_id"] == cs]

        for flat in cs_flats:
            # Filter the bookings:
            b = bookings[bookings["object"] == flat]
            logging.warning(f"Processing cleanings in flat {flat}")

            # Shift the n_guests, eta and for each flat:
            # b['n_guests'] = b['n_guests'].shift(-1, fill_value=-1)
            # b['eta'] = b['eta'].shift(-1, fill_value="Nicht gesagt")
            # b['beds'] = b['beds'].shift(-1, fill_value="Nicht gesagt")

            # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
            b.apply(add_data_snippet, axis=1, args=(dat, flat, g))
            b.apply(add_notes_snippet, axis=1, args=(notes, flat, g))

        # Once you are done with the workbook, execute the batchRequest:
        g.batch_write_to_cell(data=dat)
        g.batch_write_notes(requests=notes)
        quota += 1
        logging.warning(f"QUOTA: {quota}")

    logging.warning("Processed all cleanings within 31 days.")


def add_data_snippet(booking, data, flat, google):
    cell_range = google.get_rolling_range(unit_id=flat, date1=booking["reservation_start"], headers_rows=2, col=secrets["flats"][flat]["cleaning_col"])
    snippet = {
        "range": cell_range,
        "values": [
            [booking["n_guests"]]
        ]
    }
    data.append(snippet)


def add_notes_snippet(booking, notes, flat, google, headers_rows: int = 2):
    # Compute the ROLLING offset, based on today - 15 - headers_row:
    offset_exact = google.excel_date(booking["reservation_start"])
    offset_first = google.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
    row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date
    note_body = f"""- ANKUNFT -\n{booking['eta']}\n\n- WÜNSCHE -\n{booking['beds']}"""

    snippet = {
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": row - 1,  # -1 bc rows start excl.
                "endRowIndex": row,
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
