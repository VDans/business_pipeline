import logging
import json
import time

import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def close_dates_z(booking, z, flat, s):
    time.sleep(1)
    if s["flats"][flat]["pid_booking"] != "":
        response = z.set_availability(channel_id="1", unit_id_z=s["flats"][flat]["pid_booking"], room_id_z=s["flats"][flat]["rid_booking"], date_from=booking["reservation_start"], date_to=booking["reservation_end"], availability=0)
        logging.info(f"Closed {flat} on Booking.com from {booking['reservation_start']} to {booking['reservation_end']} with response: {response.json()['status']['returnMessage']}")

    if s["flats"][flat]["pid_airbnb"] != "":
        response1 = z.set_availability(channel_id="3", unit_id_z=s["flats"][flat]["pid_airbnb"], room_id_z=s["flats"][flat]["rid_airbnb"], date_from=booking["reservation_start"], date_to=booking["reservation_end"] + pd.Timedelta(days=-1), availability=0)
        logging.info(f"Closed {flat} on Airbnb from {booking['reservation_start']} to {booking['reservation_end']} with response: {response1.json()['status']['returnMessage']}")


def check_bookings():
    """
    This task runs once an hour.
    It checks whether the availability reflected on the platforms corresponds to what is in the database.
    It is CRITICAL to avoid over-bookings. Therefore, it runs as a standalone. The Google interface should be in a separate script.
    """

    # Get bookings table:
    sql = open("sql/task_concierge.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})

    # Get list of flats
    flats = list(bookings["object"].unique())
    # flats = ["RHG108"]
    z = Zodomus(secrets=secrets)

    for flat in flats:
        logging.info(f"Processing bookings in flat {flat}")
        b = bookings[bookings["object"] == flat]

        # For each booking (list of timestamps), send a POST request to close the dates:
        b.apply(close_dates_z, axis=1, args=(z, flat, secrets))

    logging.info("Ran the concierge successfully")


check_bookings()
