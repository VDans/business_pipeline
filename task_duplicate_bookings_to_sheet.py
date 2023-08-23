import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from google_api import Google

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
    bookings = dbh.query_data(sql=sql, dtypes={"booking_date": str, "reservation_start": str, "reservation_end": str, "phone": str})

    # 2/ Clear Google Sheet
    g.clear_range(cell_range="A2:ZZ10000")

    # 3/ Output to Google Sheet:
    data = [bookings.columns.values.tolist()]
    data.extend(bookings.values.tolist())

    g.write_table_to_cell(values=data, cell_range_start="A1")


copy_db()
