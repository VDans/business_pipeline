import json
import logging

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)

z = Zodomus(secrets=secrets)

z.set_availability(channel_id="1",
                   unit_id_z="11015792",
                   room_id_z="1101579201",
                   date_from=pd.Timestamp(day=4, month=1, year=2024),
                   date_to=pd.Timestamp(day=12, month=1, year=2024),
                   availability=0)
z.set_availability(channel_id="3",
                   unit_id_z="994746383930250287",
                   room_id_z="99474638393025028701",
                   date_from=pd.Timestamp(day=4, month=1, year=2024),
                   date_to=pd.Timestamp(day=12, month=1, year=2024),
                   availability=0)
