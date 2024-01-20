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
    The difference here is that we remove the notes, and we erase everything, not only the merging and notes, but the writing too.
    """
    logging.warning(f"The time right now is: {pd.Timestamp.now()}")

    g = Google(secrets, secrets["google"]["availability_overview_workbook_id"])
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
    g.clear_range("B3:ZZ500")
    logging.info(f"Cleared the last row")

    # 4/ Rewrite data
    n = Notes(secrets=secrets, google=g, notes=False)
    n.write_notes()


shift_sheet()
