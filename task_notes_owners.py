import json
import logging
import pandas as pd
from notes import NotesOwners

pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
logging.getLogger().setLevel(logging.INFO)

# for f in flats:
#     n = NotesOwners(secrets=secrets, workbook_key="bianca_workbook_id")
#     n.write_notes()
