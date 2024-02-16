import logging
import json
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus


logging.getLogger().setLevel(logging.INFO)

pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)
z = Zodomus(secrets=secrets)

logging.info(f"The time right now is: {pd.Timestamp.now()}")

flats = [f[0] for f in secrets["flats"].items() if f[1]["pricing_col"] != ""]


def check_prices():
    """
    B) Compare DB & Platforms
        1) Import prices from DB
        2) Import prices from Zodomus.
        3) For each date, compare 1) and 2). If !=, correct with POST /availabilities
    """
    for flat in flats:
        logging.info(f"Processing prices in flat {flat}")

        # 1) Import prices from DB:
        df_db = dbh.query_data(f"SELECT * FROM pricing WHERE price_date >= CURRENT_DATE AND object = '{flat}'")

        # 2a) Zodomus GET min. nights from BOOKING
        # First check if property on booking:
        p_check = z.check_property(channel_id="1", unit_id_z=secrets["flats"][flat]["pid_booking"]).json()
        if p_check["status"]["returnCode"] == "200":
            try:
                adjust_prices(z=z, channel_id_z="1", unit_id_z=secrets["flats"][flat]["pid_booking"], room_id_z=secrets["flats"][flat]["rid_booking"], rate_id_z=secrets["flats"][flat]["rtid_booking"], db=df_db)
            except Exception as ex:
                logging.error(f"ERROR: Could not process Booking.com with exception: {ex}")

        else:
            logging.info(f"Property NOT active on Booking.com. SKIPPING")

        # 2b) Zodomus GET min. nights from AIRBNB
        p_check = z.check_property(channel_id="3", unit_id_z=secrets["flats"][flat]["pid_airbnb"]).json()
        if p_check["status"]["returnCode"] == "200":
            try:
                adjust_prices(z=z, channel_id_z="3", unit_id_z=secrets["flats"][flat]["pid_airbnb"], room_id_z=secrets["flats"][flat]["rid_airbnb"], rate_id_z=secrets["flats"][flat]["rtid_airbnb"], db=df_db)
            except Exception as ex:
                logging.error(f"ERROR: Could not process Airbnb with exception: {ex}")

        else:
            logging.info(f"Property NOT active on Airbnb. SKIPPING")


def adjust_prices(z, channel_id_z: str, unit_id_z: str, room_id_z: str, rate_id_z: str, db):
    channel_name = "Booking.com" if channel_id_z == '1' else "Airbnb"

    logging.info(f"--- Getting availabilities from {channel_name}...")
    # The API only allows for 30 days rates check.
    init_date = pd.Timestamp(db["price_date"].min())
    info_z = []
    for i in range(1):
        logging.info(f"Init Date: {init_date}")
        # month_delta goes from 0 to 11. Number of months after the current month.
        min_z_response = z.check_availability(unit_id_z=unit_id_z, channel_id=channel_id_z, date_from=init_date, date_to=init_date + pd.Timedelta(days=30)).json()
        room_data = [r for r in min_z_response["rooms"] if r["id"] == room_id_z][0]["dates"]  # Contains min nights + prices

        # Create a list of tuples containing: date - price - min_nights - availability
        try:
            info_z += list(zip([datetime.strptime(d["date"], "%Y-%m-%d") for d in room_data], [d["rates"][0]["price"] for d in room_data], [d["rates"][0]["minStayThrough"] for d in room_data], [d["availability"] for d in room_data]))
        except TypeError:
            logging.error(f"TypeError : Cannot go further than {init_date}. Moving on with min. nights checks until that date.")
            break

        init_date += pd.Timedelta(days=30)

    # Make sure the dates ranges are the same:
    db = db[db["price_date"].isin([i[0].date() for i in info_z])]

    # Compare 1) and 2):
    db.apply(compare_prices, axis=1, args=(info_z, channel_id_z, unit_id_z, room_id_z, rate_id_z))


def compare_prices(db_row, info_z, channel_id_z, unit_id_z, room_id_z, rate_id_z):
    time.sleep(2)
    date = db_row["price_date"]
    p_db = int(db_row["price"])
    m_db = int(db_row["min_nights"])
    pz = [m for m in info_z if m[0].date() == date][0]
    p_z = int(float(pz[1]))
    m_z = int(float(pz[2]))
    m_z = 1 if m_z == 0 else m_z  # A response with min nights of '0' means there is no minimum, therefore a 'one night' minimum.
    a_z = int(float(pz[3]))

    if m_db != 0:  # If min_nights != 0 & availability in Z is OPEN
        # Min nights
        if m_db != m_z:
            logging.info(f"DELTA min nights: {date}: {m_db} vs {m_z}")
            # DB is the absolute truth
            if channel_id_z == "1":
                response = z.set_minimum_nights(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, min_nights=m_db).json()
            else:
                response = z.set_airbnb_rate(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, min_nights=m_db, price=p_db).json()

            logging.info(f"Pushed new Min. {m_db} with response: {response['status']['returnMessage']}")

        # Price
        if p_db < 30:
            logging.info(f"PRICE TOO LOW: {date} - Price {p_db} detected on the DB. Setting price at 30, not lower")
            # DB is too low. Setting to 30.
            response = z.set_rate(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, price=30).json()
            logging.info(f"Pushed new price 30 with response: {response['status']['returnMessage']}")

        if p_db != p_z:
            logging.info(f"DELTA price: {date}: {p_db} (User) vs {p_z} (Platform)")
            # Google is the absolute truth
            response = z.set_rate(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, price=p_db).json()
            logging.info(f"Pushed new price {p_db} with response: {response['status']['returnMessage']}")

    elif m_db != a_z:
        # If db_min = 0 & a_z is STILL open:
        logging.info(f"Min nights is 0, closing the night...")
        if channel_id_z == "3":
            z.set_availability(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, date_from=date, date_to=date, availability=0)
        else:
            z.set_availability(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, date_from=date, date_to=(date + pd.Timedelta(days=1)), availability=0)


check_prices()
