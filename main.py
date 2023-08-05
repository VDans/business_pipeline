import logging
import json
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from google_api import Google


def add_write_snippet(booking_date, google, data, flat, value):
    cell_range = google.get_pricing_range(unit_id=flat, date1=booking_date, col=secrets["flats"][flat]["pricing_col"])
    snippet = {
        "range": cell_range,
        "values": [
            [value]
        ]
    }
    data.append(snippet)


logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))
z = Zodomus(secrets)
g = Google(secrets=secrets, workbook_id="17c7HeZQtNGJgTaE6xUaSYFI1wkPqvnxwzLqFLXRNXps")

date_from = pd.Timestamp(day=21, month=4, year=2024)
date_to = pd.Timestamp(day=24, month=4, year=2024)
flat_name = 'LORY22'
value = "Matthias"

dates_range = pd.Series(pd.date_range(start=date_from, end=(date_to - pd.Timedelta(days=1))))

dat = []
dates_range.apply(add_write_snippet, args=(g, dat, flat_name, value))
g.batch_write_to_cell(data=dat)
print("")
