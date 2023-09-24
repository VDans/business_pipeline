import json
import logging
import pandas as pd

from google_api import Google
from notes import Notes

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))


def shift_sheet():
    """
    1x a day, shift cells up by 1.
    1/ Clear notes and merges
    2/ Shift them up by 1
    3/ Clear notes and merges on the last row (Which got added) by /2. (always row 500)
    4/ Another task will take care of the data
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

    # 3/ Clear last row
    g.clear_range("B500:ZZ500")
    logging.info(f"Cleared the last row")

    # 4/ Rewrite data
    n = Notes(secrets=secrets)
    n.write_notes()


shift_sheet()
