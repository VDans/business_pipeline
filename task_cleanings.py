import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

from google_api import Google

logging.basicConfig(level=logging.INFO)

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)
z = Zodomus(secrets=secrets)

g = Google(secrets, secrets["google"])


def write_cleanings():
    """
    This task runs once a day.

    Get flat name, checkout date, and number of guests from each reservation.
    For each, find out the correct Google workbook id and write to the correct cell according to the flat and date.
    """
    pass

