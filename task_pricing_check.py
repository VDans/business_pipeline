import logging
import json
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
from google_api import Google


logging.getLogger().setLevel(logging.INFO)

pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)
logging.info(f"The time right now is: {pd.Timestamp.now()}")

flats = [f[0] for f in secrets["flats"].items() if f[1]["pricing_col"] != ""]


def check_prices():
    """
    This task runs once a day.

    1) Import the Google Sheet prices per date.
    2) GET the prices from Zodomus.
    3) For each date, compare 1) and 2). If !=, correct with POST /availabilities
    """
    z = Zodomus(secrets=secrets)
    g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])
    for flat in flats:
        logging.info(f"----- Processing prices in flat {flat}")

        # 1a) Google Sheet Data: Min nights provide an indication of which rows needs to be watched!
        pricing_col = secrets["flats"][flat]["pricing_col"]
        ranges = [f"A3:A999", f"""{pricing_col}3:{pricing_col}999"""]
        min_gsheet = g.batch_read_cells(ranges=ranges)
        # Process dates:
        min_gsheet_dates = sum(min_gsheet[0]["values"], [])
        min_gsheet_dates = [from_excel_ordinal(d) for d in min_gsheet_dates]
        # Process min. nights:
        min_gsheet_min = [str(n[0]) if len(n) > 0 else "Booked" for n in min_gsheet[1]["values"]]
        min_gsheet = list(zip(min_gsheet_dates, min_gsheet_min))
        # Filter where open & not booked & > Today
        min_gsheet = [n for n in min_gsheet if any(char.isdigit() for char in n[1])]
        min_gsheet = [n for n in min_gsheet if n[0] >= pd.Timestamp.today()]
        min_gsheet = [n for n in min_gsheet if str(n[1]) != '0']  # Take out the 0's. The night is closed.

        min_gsheet_dates = [m[0] for m in min_gsheet]

        # 1b) Google Sheet Prices Collecting:
        min_nights_col = secrets["flats"][flat]["pricing_col"]
        n_min_nights_col = g.col2num(min_nights_col)
        pricing_col = g.n_to_col(n_min_nights_col + 1)  # Take the column first on the right.
        ranges = [f"A3:A999", f"""{pricing_col}3:{pricing_col}999"""]
        prices_gsheet = g.batch_read_cells(ranges=ranges)
        # Process dates:
        prices_gsheet_dates = sum(prices_gsheet[0]["values"], [])
        prices_gsheet_dates = [from_excel_ordinal(d) for d in prices_gsheet_dates]
        prices_gsheet_min = [str(n[0]) if len(n) > 0 else 1000 for n in prices_gsheet[1]["values"]]
        prices_gsheet = list(zip(prices_gsheet_dates, prices_gsheet_min))
        prices_gsheet = [p for p in prices_gsheet if p[0] in min_gsheet_dates]  # Only keep the prices where the property is not booked or closed

        # 2a) Zodomus GET min. nights from BOOKING
        # First check if property on booking:
        p_check = z.check_property(channel_id="1", unit_id_z=secrets["flats"][flat]["pid_booking"]).json()
        if p_check["status"]["returnCode"] == "200":
            try:
                adjust_prices(z=z, channel_id_z="1", unit_id_z=secrets["flats"][flat]["pid_booking"], room_id_z=secrets["flats"][flat]["rid_booking"], rate_id_z=secrets["flats"][flat]["rtid_booking"], prices_gsheet=prices_gsheet)
            except Exception as ex:
                logging.error(f"ERROR: Could not process Booking.com with exception: {ex}")

        else:
            logging.info(f"Property NOT active on Booking.com. SKIPPING")

        # 2b) Zodomus GET min. nights from AIRBNB
        p_check = z.check_property(channel_id="3", unit_id_z=secrets["flats"][flat]["pid_airbnb"]).json()
        if p_check["status"]["returnCode"] == "200":
            try:
                adjust_prices(z=z, channel_id_z="3", unit_id_z=secrets["flats"][flat]["pid_airbnb"], room_id_z=secrets["flats"][flat]["rid_airbnb"], rate_id_z=secrets["flats"][flat]["rtid_airbnb"], prices_gsheet=prices_gsheet)
            except Exception as ex:
                logging.error(f"ERROR: Could not process Airbnb with exception: {ex}")

        else:
            logging.info(f"Property NOT active on Airbnb. SKIPPING")


def adjust_prices(z, channel_id_z: str, unit_id_z: str, room_id_z: str, rate_id_z: str, prices_gsheet):
    channel_name = "Booking.com" if channel_id_z == '1' else "Airbnb"

    logging.info(f"--- Getting availabilities from {channel_name}...")
    # The API only allows for 30 days rates check.
    init_date = prices_gsheet[0][0]
    price_z = []
    for i in range(13):
        logging.info(f"Init Date: {init_date}")
        # month_delta goes from 0 to 11. Number of months after the current month.
        time.sleep(1)
        min_z_response = z.check_availability(unit_id_z=unit_id_z, channel_id=channel_id_z, date_from=init_date, date_to=init_date + pd.Timedelta(days=30)).json()
        room_data = [r for r in min_z_response["rooms"] if r["id"] == room_id_z][0]["dates"]
        try:
            price_z += list(zip([datetime.strptime(d["date"], "%Y-%m-%d") for d in room_data], [d["rates"][0]["price"] for d in room_data]))
        except TypeError:
            logging.error(f"TypeError: Cannot go further than {init_date}. Moving on with price checks until that date.")
            break
        init_date += pd.Timedelta(days=30)


    # Make sure the dates ranges are the same:
    prices_gsheet = [m1 for m1 in prices_gsheet if m1[0] in [m2[0] for m2 in price_z]]

    # Compare 1) and 2)
    logging.info(f"- Comparing the Google Sheet and {channel_name}...")
    for d in prices_gsheet:
        date = d[0]
        p_g = int(d[1])
        pz = [m for m in price_z if m[0] == date][0]
        p_z = int(float(pz[1]))

        if p_g < 30:
            logging.info(f"PRICE TOO LOW: {date} - Price {p_g} detected on the Google Sheet. Setting price at 30, not lower")
            # Google is too low. Setting to 30.
            time.sleep(1)
            response = z.set_rate(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, price=30).json()
            logging.info(f"Pushed new price 30 to {channel_name} with response: {response['status']['returnMessage']}")

        if p_g != p_z:
            logging.info(f"DELTA: {date}: {p_g} (User) vs {p_z} (Platform)")
            # Google is the absolute truth
            time.sleep(1)
            response = z.set_rate(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, price=p_g).json()
            logging.info(f"Pushed new price {p_g} to {channel_name} with response: {response['status']['returnMessage']}")


def check_minimum_nights():
    """
    This task runs once a day.

    1) Import the Google Sheet minimum nights per date.
    2) GET /rates with the minimum nights from Zodomus.
    3) For each date, compare 1) and 2). If !=, correct with POST /rates
    """
    z = Zodomus(secrets=secrets)
    g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])
    for flat in flats:
        logging.info(f"----- Processing minimum nights in flat {flat}")

        # 1) Google Sheet Data
        pricing_col = secrets["flats"][flat]["pricing_col"]
        ranges = [f"A3:A999", f"""{pricing_col}3:{pricing_col}999"""]
        min_gsheet = g.batch_read_cells(ranges=ranges)
        # Process dates:
        min_gsheet_dates = sum(min_gsheet[0]["values"], [])
        min_gsheet_dates = [from_excel_ordinal(d) for d in min_gsheet_dates]
        # Process min. nights:
        min_gsheet_min = [str(n[0]) if len(n) > 0 else "Booked" for n in min_gsheet[1]["values"]]
        min_gsheet = list(zip(min_gsheet_dates, min_gsheet_min))
        # Filter where open & not booked & > Today
        min_gsheet = [n for n in min_gsheet if any(char.isdigit() for char in n[1])]
        min_gsheet = [n for n in min_gsheet if n[0] >= pd.Timestamp.today()]
        min_gsheet = [n for n in min_gsheet if str(n[1]) != '0']  # Take out the 0's. The night is closed.

        # 2a) Zodomus GET min. nights from BOOKING
        # First check if property on booking:
        p_check = z.check_property(channel_id="1", unit_id_z=secrets["flats"][flat]["pid_booking"]).json()
        if p_check["status"]["returnCode"] == "200":
            try:
                adjust_min_nights(z=z, channel_id_z="1", unit_id_z=secrets["flats"][flat]["pid_booking"], room_id_z=secrets["flats"][flat]["rid_booking"], rate_id_z=secrets["flats"][flat]["rtid_booking"], min_gsheet=min_gsheet)
            except Exception as ex:
                logging.error(f"ERROR: Could not process Booking.com with exception: {ex}")

        else:
            logging.info(f"Property NOT active on Booking.com. SKIPPING")

        # 2b) Zodomus GET min. nights from AIRBNB
        p_check = z.check_property(channel_id="3", unit_id_z=secrets["flats"][flat]["pid_airbnb"]).json()
        if p_check["status"]["returnCode"] == "200":
            try:
                adjust_min_nights(z=z, channel_id_z="3", unit_id_z=secrets["flats"][flat]["pid_airbnb"], room_id_z=secrets["flats"][flat]["rid_airbnb"], rate_id_z=secrets["flats"][flat]["rtid_airbnb"], min_gsheet=min_gsheet)
            except Exception as ex:
                logging.error(f"ERROR: Could not process Airbnb with exception: {ex}")

        else:
            logging.info(f"Property NOT active on Airbnb. SKIPPING")


def adjust_min_nights(z, channel_id_z: str, unit_id_z: str, room_id_z: str, rate_id_z: str, min_gsheet):
    channel_name = "Booking.com" if channel_id_z == '1' else "Airbnb"

    logging.info(f"Getting availabilities from {channel_name}...")
    # The API only allows for 30 days rates check.
    init_date = min_gsheet[0][0]
    min_z = []
    for i in range(13):
        logging.info(f"Init Date: {init_date}")
        # month_delta goes from 0 to 11. Number of months after the current month.
        time.sleep(1)
        min_z_response = z.check_availability(unit_id_z=unit_id_z, channel_id=channel_id_z, date_from=init_date, date_to=init_date + pd.Timedelta(days=30)).json()
        room_data = [r for r in min_z_response["rooms"] if r["id"] == room_id_z][0]["dates"]
        try:
            min_z += list(zip([datetime.strptime(d["date"], "%Y-%m-%d") for d in room_data], [d["rates"][0]["minStayThrough"] for d in room_data]))
        except TypeError:
            logging.error(
                f"TypeError: Cannot go further than {init_date}. Moving on with min. nights checks until that date.")
            break
        init_date += pd.Timedelta(days=30)

    # Make sure the dates ranges are the same:
    min_gsheet = [m1 for m1 in min_gsheet if m1[0] in [m2[0] for m2 in min_z]]

    # Compare 1) and 2)
    logging.info(f"Comparing the Google Sheet and {channel_name}...")
    for d in min_gsheet:
        date = d[0]
        m_night_g = int(d[1])
        mz = [m for m in min_z if m[0] == date][0]
        m_night_z = 1 if mz[1] == "0" else int(mz[1])  # A response with min nights of '0' means there is no minimum, therefore a 'one night' minimum.
        if m_night_g != m_night_z:
            logging.info(f"DELTA: {date}: {m_night_g} vs {m_night_z}")

            # Google is the absolute truth
            time.sleep(1)
            if channel_id_z == "1":
                response = z.set_minimum_nights(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, min_nights=m_night_g).json()
            else:
                response = z.set_airbnb_rate(channel_id=channel_id_z, unit_id_z=unit_id_z, room_id_z=room_id_z, rate_id_z=rate_id_z, date_from=date, min_nights=m_night_g, price=1000).json()

            logging.info(f"Pushed new Min. {m_night_g} to {channel_name} with response: {response['status']['returnMessage']}")


def from_excel_ordinal(ordinal: float, _epoch0=datetime(1899, 12, 31)) -> datetime:
    if ordinal >= 60:
        ordinal -= 1  # Excel leap year bug, 1900 is not a leap year!
    return (_epoch0 + pd.Timedelta(days=ordinal)).replace(microsecond=0)


check_minimum_nights()
check_prices()
