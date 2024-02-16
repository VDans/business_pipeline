import json
import logging
import pandas as pd

from notes_horizontal import NotesH
from google_api import Google

pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
logging.getLogger().setLevel(logging.INFO)

g = Google(secrets=secrets, workbook_id=secrets["google"]["availability_overview_workbook_id"])
n = NotesH(secrets=secrets, google=g)
n.write_notes()
