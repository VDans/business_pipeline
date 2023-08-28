import pandas as pd
import logging
import json
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from google_api import Google

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))


def shift_sheet():
    """
    1x a day, shift cells up by 1.
    1/ Clear notes and merges
    2/ Shift them up by 1
    3/ Clear notes and merges on the last row (Which got added) by /2. (always row 500)
    3/ Another task will take care of the data
    """
    logging.warning(f"The time right now is: {pd.Timestamp.now()}")

    g = Google(secrets, secrets["google"]["pricing_workbook_id"])
    sheet_id = 920578163
    
    # /1
    g.write_note(0, 500, 0, 100, "", sheet_id)
    g.unmerge_cells(0, 500, 0, 100, sheet_id)
    logging.info(f"Cleared all notes and merges")

    # /2
    r = [
        {
            "moveDimension": {
                "source": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 3,
                    "endIndex": 500
                },
                "destinationIndex": 2
            }
        }
    ]

    g.batch_request(requests=r)
    logging.info(f"Shifted all cells up by 1 row")

    g.clear_range("B500:ZZ500")
    logging.info(f"Cleared the last row")


shift_sheet()


def write_notes():
    """
    Used when you need to correct the notes on the pricing sheet
    """
    logging.warning(f"The time right now is: {pd.Timestamp.now()}")

    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    sql = open("sql/task_notes.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})
    g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])

    # Get list of flats
    flats = [f[0] for f in secrets["flats"].items() if f[1]["pricing_col"] != ""]

    # Clear workbook:
    g.write_note(0, 998, 0, 100, "", 920578163)
    g.unmerge_cells(0, 999, 0, 100, 920578163)

    dat = []
    merg = []
    notes = []

    for flat in flats:
        logging.warning(f"Processing notes in flat {flat}")

        logging.warning(f"Cleared worksheet of values and notes.")

        # Filter the bookings:
        b = bookings[bookings["object"] == flat]

        # Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
        b.apply(add_write_snippet, axis=1, args=(dat, flat, g, 920578163))
        b.apply(add_notes_snippet, axis=1, args=(notes, flat, g, 920578163))
        b.apply(add_merge_snippet, axis=1, args=(merg, flat, g, 920578163))

    # Once you are done with the workbook, execute the batchRequest:
    # Write cell values
    g.batch_write_to_cell(data=dat)
    # Write notes
    g.batch_write_notes(requests=notes)
    # Merge booking cells
    g.batch_request(requests=merg)

    logging.warning("Processed all notes for this flat.")


def add_write_snippet(booking, data, flat, google, internal_sheet_id):
    cell_range = google.get_rolling_range(unit_id=flat, date1=booking["reservation_start"], headers_rows=3, col=secrets["flats"][flat]["pricing_col"])
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


def add_notes_snippet(booking, notes, flat, google, internal_sheet_id, headers_rows: int = 3):
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


def add_merge_snippet(booking, merg, flat, google, internal_sheet_id, headers_rows: int = 3):
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
                "startColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]),
                "endColumnIndex": google.col2num(secrets["flats"][flat]["pricing_col"]) + 1
            },
            "mergeType": "MERGE_ALL"
        }
    }
    logging.warning(f"Merge Snippet: {snippet}")
    merg.append(snippet)


write_notes()
