import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

from google_api import Google

logging.basicConfig(level=logging.INFO)

secrets = json.load(open('config_secrets.json'))
# db_engine = create_engine(url=secrets["database"]["url"])
# dbh = DatabaseHandler(db_engine, secrets)
# z = Zodomus(secrets=secrets)
g = Google(secrets)
date1 = pd.Timestamp(day=1, month=6, year=2023)
date2 = pd.Timestamp(day=3, month=6, year=2023)

# print(g.excel_date(date1) - 45075)

# g.merge_cells2(date1, date2, "OMG10")
g.unmerge_cells2(date1, date2, "OMG10")

# print(response)
# print(json.dumps(response.json(), indent=3))
