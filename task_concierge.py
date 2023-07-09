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


def check_bookings():
    """
    This task runs once an hour.

    It checks whether the availability reflected on the platforms corresponds to what is written on the Pricing Sheet.

    - For each flat, get list of dates where it should be CLOSED.
    - In Google Sheet, check if value == 0
    - If value > 0: Send Email.
    """
    # Get closed dates:
    sql = open("sql/task_concierge.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})
    # Important! Remove 1 day from end dates!
    bookings["reservation_end"] = bookings["reservation_end"] - pd.Timedelta(days=1)

    flats = list(bookings["object"].unique())
    g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])

    for flat in flats:
        logging.info(f"Processing cleanings in flat {flat}")
        b = bookings[bookings["object"] == flat]
        closed_dates = b.apply(lambda x: list(pd.date_range(x['reservation_start'], x['reservation_end'])), axis=1)
        closed_dates = sum(closed_dates, [])  # Get all closed dates

        # Check each value if 0, "Booking", "Airbnb", or "Booked":
        for d in closed_dates:
            cell_range = g.get_pricing_range(unit_id=flat, date1=d, col=secrets["flats"][flat]["pricing_col"])
            value = g.read_cell(cell_range=cell_range)

            logging.info(f"Checking {flat} - {d} - {cell_range}")
            if value not in [0, "0", "Booking", "Airbnb", "Booked", "", " "]:
                logging.warning(f"flat {flat} date {d} is opened with value {value}, but SHOULD NOT BE! Closing it now...")
                g.write_to_cell(cell_range=cell_range, value=0)

    logging.info("Ran the concierge successfully")


check_bookings()

