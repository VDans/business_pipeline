import json
import logging
import pandas as pd

from notes import Notes
from google_api import Google

pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
logging.getLogger().setLevel(logging.INFO)

g = Google(secrets=secrets, workbook_id=secrets["google"]["availability_overview_workbook_id"])
n = Notes(secrets=secrets, google=g, notes=False)
n.write_notes()
