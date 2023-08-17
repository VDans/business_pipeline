import logging
import json
import time
import string
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from google_api import Google

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def remind_guest():
    """
    This task runs once a day at 11:00. It:
    1) Identify guests checking in exactly 3 days.
    2) Send email + whatsapp template / SMS to provided number, giving them their booking number.
    """
    bookings_3_days = ""
    variable_platform = ""
    message_body = \
f"""Hello!
This is the host for your apartment in Vienna.
We noticed that you have not seen our messages on {variable_platform}.
Therefore, we send you here a reminder to fill our online check-in form.
Once this is done, you will receive precise instructions on {variable_platform} on how to enter your flat.
Have a good day!"""

