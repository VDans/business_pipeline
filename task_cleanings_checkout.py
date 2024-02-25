import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from google_api import Google

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def write_cleanings():
    """
    This task runs once an hour.

    Get flat name, checkout date, and number of guests from each reservation.
    For each, find out the correct Google workbook id and write to the correct cell according to the flat and date.

    This should be done using batchRequests!
    """
    sql = open("sql/task_cleanings_checkout.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_end": pd.Timestamp})

    flats = [f[0] for f in secrets["flats"].items() if "cleaning_workbook_id" in f[1]]
    cleaning_sheets = list(set([secrets["flats"][f[0]]["cleaning_workbook_id"] for f in secrets["flats"].items() if "cleaning_workbook_id" in f[1]]))
    cleaning_sheets.remove("1tbWy6dMqEp4zKIJquKJhpKBdfSqiiEVufvQ0on3dRss")
    cleaning_sheets.remove("1uykPKNoVBzj5seAuiaYKUBmdK3W5i2ZyabDTp3K1MJg")
    logging.info(f"The time right now is: {pd.Timestamp.now()}")

    for cs in cleaning_sheets:
        dat = []
        notes = []

        # Find the corresponding Google sheet for the specific cleaner:
        g = Google(secrets=secrets, workbook_id=cs)

        # Clear workbook:
        response = g.clear_range(cell_range="B2:Z1000")
        # Clear notes: Make a large batch with notes to "":
        response1 = g.write_note(0, 900, 0, 50, "", 0)
        logging.info(f"Cleared worksheet of values and notes.")

        # Find all flats on this workbook:
        cs_flats = [f for f in flats if secrets["flats"][f]["cleaning_workbook_id"] == cs]

        for flat in cs_flats:
            # Filter the bookings:
            b = bookings[bookings["object"] == flat]
            logging.info(f"Processing cleanings in flat {flat}")

            # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
            b.apply(add_data_snippet, axis=1, args=(dat, flat, g))
            b.apply(add_notes_snippet, axis=1, args=(notes, flat, g))

        # Once you are done with the workbook, execute the batchRequest:
        g.batch_write_to_cell(data=dat)
        g.batch_write_notes(requests=notes)

    logging.info("Processed all cleanings within 31 days.")


def add_data_snippet(booking, data, flat, google):
    cell_range = google.get_rolling_range(unit_id=flat, date1=booking["reservation_end"], headers_rows=2, col=secrets["flats"][flat]["cleaning_col"])
    snippet = {
        "range": cell_range,
        "values": [
            [booking["n_guests"]]
        ]
    }
    data.append(snippet)


def add_notes_snippet(booking, notes, flat, google, headers_rows: int = 2):
    # Compute the ROLLING offset, based on today - 15 - headers_row:
    offset_exact = google.excel_date(booking["reservation_end"])
    offset_first = google.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
    row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date
    note_body = f"""- FREI AB -\n{booking['etd']}\n\n- ANKUNFT -\n{booking['eta']}\n\n- WÜNSCHE -\n{booking['beds']}"""

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
