import logging
import json
import gspread
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from google_api import Google
from gspread_dataframe import set_with_dataframe

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def copy_db():
    """
    This task reads the bookings table from the DB, and outputs it to a Google Sheet.
    """
    g = Google(secrets=secrets, workbook_id=secrets["google"]["bookings_workbook_id"])

    # 1/ Read Bookings
    sql = open("sql/task_bookings.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"booking_date": str, "reservation_start": str, "reservation_end": str, "phone": str, "end_of_month_start": str, "end_of_month_end": str})

    # 2/ Clear Google Sheet
    g.clear_range(cell_range="A1:ZZ10000")

    # 3/ Output to Google Sheet:
    gc = gspread.service_account("google_secrets.json")
    sh = gc.open_by_key(secrets["google"]["bookings_workbook_id"]).sheet1
    sh.update([bookings.columns.values.tolist()] + bookings.values.tolist())


copy_db()
