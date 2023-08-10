import logging
import json
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
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})
    g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])

    # Get list of flats
    flats = [f[0] for f in secrets["flats"].items() if f[1]["pricing_col"] != ""]
    # flats = ["LORY22"]

    # Clear workbook:
    g.write_note(0, 998, 0, 100, "", 920578163)
    g.unmerge_cells(0, 999, 0, 100, 920578163)
    dat = []
    merg = []
    # colors = []
    notes = []

    for flat in flats:
        logging.info(f"Processing notes in flat {flat}")

        logging.info(f"Cleared worksheet of values and notes.")

        # Filter the bookings:
        b = bookings[bookings["object"] == flat]

        # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
        b.apply(add_write_snippet, axis=1, args=(dat, flat, g, 920578163))
        b.apply(add_notes_snippet, axis=1, args=(notes, flat, g, 920578163))
        # b.apply(add_color_snippet, axis=1, args=(colors, flat, g, 920578163))
        b.apply(add_merge_snippet, axis=1, args=(merg, flat, g, 920578163))

    # Once you are done with the workbook, execute the batchRequest:
    # Write cell values
    g.batch_write_to_cell(data=dat)
    # Write notes
    g.batch_write_notes(requests=notes)
    # Write colors
    # g.batch_request(requests=colors)
    # Merge booking cells
    g.batch_request(requests=merg)

    logging.info("Processed all notes for this flat.")


def add_write_snippet(booking, data, flat, google, internal_sheet_id, offset=45075):
    cell_range = google.get_pricing_range(unit_id=flat, date1=booking["reservation_start"], col=secrets["flats"][flat]["pricing_col"], offset=offset)
    snippet = {
        "range": cell_range,
        "values": [
            [booking["platform"]]
        ]
    }
    data.append(snippet)


def add_notes_snippet(booking, notes, flat, google, internal_sheet_id, offset=45075):
    note_body = f"""{booking["guest_name"].title()}\nPaid {booking["total_amount_paid_by_guest"]}€\nGuests: {booking["n_guests"]}\nID: {booking["booking_id"]}"""

    snippet = {
        "updateCells": {
            "range": {
                "sheetId": internal_sheet_id,
                "startRowIndex": google.excel_date(booking["reservation_start"]) - offset - 1,
                "endRowIndex": google.excel_date(booking["reservation_start"]) - offset,
                "startColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]),
                "endColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]) + 1
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


def add_merge_snippet(booking, merg, flat, google, internal_sheet_id, offset=45075):
    snippet = {
        "mergeCells": {
            "range": {
                "sheetId": internal_sheet_id,
                "startRowIndex": google.excel_date(booking["reservation_start"]) - offset - 1,
                "endRowIndex": google.excel_date(booking["reservation_end"]) - offset - 1,
                "startColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]),
                "endColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]) + 1
            },
            "mergeType": "MERGE_ALL"
        }
    }
    merg.append(snippet)


def add_color_snippet(booking, color, flat, google, internal_sheet_id, offset=45075):
    snippet = {
        "updateCells": {
            "rows": [
                {
                    "values": [{
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 58,
                                "green": 80,
                                "blue": 92,
                                "alpha": 1
                            }}}
                    ]
                }
            ],
            "fields": 'userEnteredFormat.backgroundColor',
            "range": {
                "sheetId": internal_sheet_id,
                "startRowIndex": google.excel_date(booking["reservation_start"]) - offset - 1,
                "endRowIndex": google.excel_date(booking["reservation_start"]) - offset,
                "startColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]),
                "endColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]) + 1
            }
        }
    }
    color.append(snippet)


write_notes()
