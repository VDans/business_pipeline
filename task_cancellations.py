import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

pd.options.mode.chained_assignment = None


def check_booking(booking, z):
    """
    1. The cleaning fee should be on 0
    2. The nights fee should be = to response["reservations"]["rooms"][0]["totalPrice"]
    """
    channel_id_z = "1" if booking["platform"] == '1' else "3"
    logging.info(f"Checking booking_id = {booking['booking_id']}")
    # z = Zodomus(secrets=secrets)
    # z_response = z.get_reservation(channel_id=channel_id_z, unit_id_z=)
    pass


def correct_cancellations():
    """
    This task goes over past cancellations, and checks whether the correct payout is in the DB.
    """
    logging.basicConfig(level=logging.INFO)

    secrets = json.load(open('config_secrets.json'))
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)
    z = Zodomus(secrets=secrets)

    sql = open("sql/task_cancellations.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"reservation_start": pd.Timestamp})

    # Go over the bookings and check:
    bookings.apply(check_booking, axis=1, args=(z, ))


correct_cancellations()
