import json
import logging
import pandas as pd
from notes import Notes

pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
logging.getLogger().setLevel(logging.INFO)

n = Notes(secrets=secrets)
n.write_notes()
