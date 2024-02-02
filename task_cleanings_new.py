from notes import Notes
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


def task_cleaning_new():
    """
    This new format will allow cleaning teams to see arrival date and time, departure date and time, number of guests, bed organisation wishes.
    """

    sql = open("sql/task_cleanings_new.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start_adjusted": pd.Timestamp, "reservation_end": pd.Timestamp})
    quota = 0

    flats = [f[0] for f in secrets["flats"].items() if f[1]["pid_booking"] != ""]
    # cleaning_sheets = list(set([secrets["flats"][f]["cleaning_workbook_id"] for f in flats if secrets["flats"][f]["cleaning_workbook_id"] != ""]))
    # FOR NOW ONLY FATMA & LEON!
    cleaning_sheets = ["1tbWy6dMqEp4zKIJquKJhpKBdfSqiiEVufvQ0on3dRss", "1uykPKNoVBzj5seAuiaYKUBmdK3W5i2ZyabDTp3K1MJg"]
    logging.info(f"The time right now is: {pd.Timestamp.now()}")

    for cs in cleaning_sheets:
        dat = []
        notes = []
        merg = []

        # Find the corresponding Google sheet for the specific cleaner:
        g = Google(secrets=secrets, workbook_id=cs)

        g.clear_range(cell_range="B2:Z1000")
        g.write_note(0, 900, 0, 50, "", 0)
        g.unmerge_cells(0, 500, 0, 100, 0)
        logging.info(f"Cleared All Infos on the Sheet")

        # Find the flats in this sheet
        cs_flats = [f for f in secrets['flats'] if secrets["flats"][f]["cleaning_workbook_id"] == cs]

        for flat in cs_flats:
            # Filter the bookings:
            b = bookings[bookings["object"] == flat]
            logging.info(f"Processing cleanings in flat {flat}")

            # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
            b.apply(add_write_snippet, axis=1, args=(dat, flat, g))
            b.apply(add_notes_snippet, axis=1, args=(notes, flat, 0, g))
            b.apply(add_merge_snippet, axis=1, args=(merg, flat, 0, g))

        # Once you are done with the workbook, execute the batchRequest:
        # Write cell values
        g.batch_write_to_cell(data=dat)
        quota += 1
        # Write notes
        g.batch_write_notes(requests=notes)
        quota += 1
        # Merge booking cells
        g.batch_request(requests=merg)
        quota += 1

        logging.info("Processed all notes for this flat.")


def add_write_snippet(booking, data, flat, g):
    cell_range = g.get_rolling_range(unit_id=flat, date1=booking["reservation_start_adjusted"], headers_rows=2, col=secrets["flats"][flat]["cleaning_col"])
    snippet = {
        "range": cell_range,
        "values": [
            [f"""Gäste: {booking["n_guests"]}"""]
        ]
    }
    data.append(snippet)


def add_notes_snippet(booking, notes, flat, internal_sheet_id, g, headers_rows: int = 2):
    # Compute the ROLLING offset, based on today - 15 - headers_row:
    offset_exact = g.excel_date(booking["reservation_start_adjusted"])
    offset_first = g.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
    row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

    note_body = f"""Gäste: {booking["n_guests"]}\n{booking["reservation_start_adjusted"].strftime("%Y-%m-%d")} bis {booking["reservation_end"].strftime("%Y-%m-%d")}\nCheck-In: {booking["eta"]}\nCheck-Out: {booking["etd"]}\nWünsche: {booking["beds"]}"""

    snippet = {
        "updateCells": {
            "range": {
                "sheetId": internal_sheet_id,
                "startRowIndex": row - 1,
                "endRowIndex": row,
                "startColumnIndex": g.col2num(secrets["flats"][flat]["cleaning_col"]),
                "endColumnIndex": g.col2num(secrets["flats"][flat]["cleaning_col"]) + 1
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


def add_merge_snippet(booking, merg, flat, internal_sheet_id, g, headers_rows: int = 2):
    # Compute the ROLLING offset, based on today - 15 - headers_row:
    offset_exact = g.excel_date(booking["reservation_start_adjusted"])
    offset_first = g.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
    row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

    snippet = {
        "mergeCells": {
            "range": {
                "sheetId": internal_sheet_id,
                "startRowIndex": row - 1,
                "endRowIndex": row + int((booking["reservation_end"] - booking["reservation_start_adjusted"]).days) - 1,
                "startColumnIndex": g.col2num(secrets["flats"][flat]["cleaning_col"]),
                "endColumnIndex": g.col2num(secrets["flats"][flat]["cleaning_col"]) + 1
            },
            "mergeType": "MERGE_ALL"
        }
    }
    merg.append(snippet)


task_cleaning_new()
