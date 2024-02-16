import json
import logging
import time
from zodomus_api import Zodomus
import pandas as pd
from datetime import datetime
from flask import Flask, request, redirect, url_for, render_template
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from sqlalchemy import create_engine, Table, MetaData, insert, delete, types
from database_handling import DatabaseHandler

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["300 per minute"]
)


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return str("Welcome to the PriceLabs Web App of Host-It Immobilien GmbH")


@app.route('/pricing_sheet', methods=['POST'])
def get_prices():
    """
    This endpoint arrives from Google, and is meant to update prices on the DB table, and then push to the platforms.
    The prices get pushed with a confirmation button, in "batches".
    This call then uploads to the DB the new changes, and then pushes live to the platforms.
    """
    start_time = time.time()

    data = request.json
    logging.info("--------------------------------------------------------------------------------------------------------")
    logging.info("PRICING New Request-------------------------------------------------------------------------------------")

    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)
    pricing_tbl = Table("pricing", MetaData(), autoload_with=db_engine)
    z = Zodomus(secrets=secrets)

    # 1. SHEET TO DATABASE
    # FixMe: First slow but easy method. Then improve on this
    df_google = pd.DataFrame(columns=["price_date", "object", "price", "min_nights"])

    for i in range(1, len(data)):
        if data[i][0] != "":
            dict_temp = {
                "change_date": pd.Timestamp.now(),
                "price_date": [pd.to_datetime(d).date() for d in data[0][2:]],
                "object": data[i][0],
                "price": data[i][2:],
                "min_nights": data[(i+1)][2:],
                "overwritten": True
            }
            df_google = pd.concat([df_google, pd.DataFrame(dict_temp)], ignore_index=True)

    # Take out the price dates where there is a booking:
    df_google.loc[df_google['min_nights'] == "", "price"] = 1000
    df_google.loc[df_google['min_nights'] == "", "min_nights"] = 0

    # Filter after today
    df_google = df_google[df_google["price_date"] >= datetime.today().date()]

    # Now prepare the database table:
    df_db = dbh.query_data(f"SELECT * FROM pricing WHERE price_date >= CURRENT_DATE")

    # Add the new rows. If two rows are the same price date and object, keep the Google one (latest!):
    df_diff = pd.concat([df_google, df_db], ignore_index=True).drop_duplicates(subset=["price_date", "object", "price", "min_nights"], keep=False)
    # Only the new changes remain. Now only keep the latest changed value:
    df_diff = df_diff.drop_duplicates(subset=["price_date", "object"], keep="first").reset_index(drop=True)

    # Update is not a viable solution, as sometimes you need to add rows with new dates!
    # Delete where the index matches:
    if len(df_diff) > 0:
        app.logger.info(f"Delta Rows Found: {df_diff}")
        with db_engine.begin() as conn:
            for i in range(len(df_diff)):
                data1 = {
                    "value_type": "Price",
                    "new_value": df_diff['price'][i],
                    "date": df_diff["price_date"][i],
                    "flat_name": df_diff["object"][i]
                }
                data2 = {
                    "value_type": "Min.",
                    "new_value": df_diff["min_nights"][i],
                    "date": df_diff["price_date"][i],
                    "flat_name": df_diff["object"][i],
                    "rightCellValue": 1000
                }
                delete_query = delete(pricing_tbl).where(pricing_tbl.c.object == data1["flat_name"], pricing_tbl.c.price_date == data1["date"])
                conn.execute(delete_query)
                app.logger.info(f"Object {data1['flat_name']} on the {data1['date']}: New price of {data1['new_value']} and min. nights of {data2['new_value']}")

                # 2. DATABASE TO PLATFORMS:
                # Push first the minimum nights:
                m_nights_to_platform(z, data2)
                # Then the price:
                price_to_platform(z, data1)

            df_diff.to_sql(
                index=None,
                dtype={
                    "price_date": types.DATE,
                    "object": types.VARCHAR(30),
                    "price": types.INTEGER,
                    "min_nights": types.INTEGER,
                    "change_date": types.TIMESTAMP,
                    "overwritten": types.BOOLEAN
                },
                con=conn,
                name='pricing',
                if_exists='append'
            )

    else:
        logging.info(f"No change detected!")

    dbh.close_engine()
    end_time = time.time()
    logging.info(f"This took {end_time - start_time} seconds")

    return str("Thanks Google!")


@app.route('/sync', methods=['POST'])
def sync_url():
    """
    This endpoint needs to be created and controlled by the PMS.
    The PMS should provide this endpoint to PriceLabs using /integration endpoint
    PriceLabs will call this endpoint and push the listings along with the associated prices and settings as mentioned in the Pricing Features in this document.
    If the sync URL trigger fails, we retry it 10 times on that calendar day.

    {
      "listing_id": "hds1",
      "last_refreshed": "2024-01-01",
      "data": [
        {
          "price": 0,
          "date": "string",
          "min_stay": 0,
          "check_in": true,
          "check_out": true,
          "weekly_discount": 0,
          "monthly_discount": 0,
          "extra_person_fee": 0,
          "extra_person_fee_trigger": 0,
          "los_discount_v2": {
            "los": {
              "los_night": "1",
              "max_price": "string",
              "min_price": "string",
              "los_adjustment": "10"
            }
          }
        }
      ]
    }
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("SYNC New Request------------------------------------------------------------------------")
    # flat_name = data["listing_id"]
    # logging.info(f"Flat Name: {flat_name}")

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


@app.route('/calendar_trigger', methods=['POST'])
def trigger_calendar():
    """
    This endpoint needs to be created and controlled by the PMS.
    The PMS should provide this endpoint to PriceLabs using /integration endpoint
    This endpoint will receive listing_ids which are in need of a full refresh of their information.
    Once this endpoint is triggered by PriceLabs, the PMS should send a full calendar including the latest prices and availability using the /calendar endpoint.

    {
      "start_date": "string",
      "end_date": "string",
      "listing_id": "string"
    }
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("CALENDAR New Request------------------------------------------------------------------------")
    logging.info(data)

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


@app.route('/hook', methods=['POST'])
def hook_url():
    """
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("HOOK New Request------------------------------------------------------------------------")
    logging.info(data)

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


def m_nights_to_platform(z, data):
    start_time = time.time()
    try:
        # Find out the booking and airbnb propertyId
        flat_name = data["flat_name"]
        property_id_airbnb = secrets["flats"][flat_name]["pid_airbnb"]
        room_id_airbnb = secrets["flats"][flat_name]["rid_airbnb"]
        rate_id_airbnb = secrets["flats"][flat_name]["rtid_airbnb"]
        property_id_booking = secrets["flats"][flat_name]["pid_booking"]
        room_id_booking = secrets["flats"][flat_name]["rid_booking"]
        rate_id_booking = secrets["flats"][flat_name]["rtid_booking"]

    except KeyError:
        logging.error(f"Could not find flat name: {data['flat_name']}, is it possible you're adding columns?")
        return str("Thanks Google.")

    if str(data["new_value"]) == "0":
        # 3. If min_nights = 0: Close the room for the night in both channels
        logging.info("Min. Nights set to 0. Closing the room.")
        z.set_availability(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, date_from=data["date"], date_to=(data["date"] + pd.Timedelta(days=1)), availability=0)
        z.set_availability(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, date_from=data["date"], date_to=data["date"], availability=0)

    else:
        logging.info("Min. Nights is not 0. Opening the room.")
        # 1. Make sure the dates are open.
        # Why? Because if min nights was on 0, and you change the min nights, the nights stay closed.
        z.set_availability(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, date_from=data["date"], date_to=(data["date"] + pd.Timedelta(days=1)), availability=1)
        z.set_availability(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, date_from=data["date"], date_to=data["date"], availability=1)

        # 2. Change the minimum nights on the platforms
        # UNFORTUNATELY the shitty Airbnb API requires a price push at the same time as the minimum nights' push.
        # Therefore, you also have to communicate the price next to the min nights requirements...
        logging.info(f"Pushing min. nights value")
        z.set_minimum_nights(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=data["date"], min_nights=data["new_value"])
        z.set_airbnb_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=data["date"], price=1000, min_nights=data["new_value"])  # Fucking hate this...

    logging.info("Pushed new min. nights")
    end_time = time.time()
    logging.info(f"This took {end_time - start_time} seconds")
    app.logger.info(f"Min nights call took {end_time - start_time} seconds")


def price_to_platform(z, data):
    start_time = time.time()

    try:
        # Find out the booking and airbnb propertyId
        flat_name = data["flat_name"]
        property_id_airbnb = secrets["flats"][flat_name]["pid_airbnb"]
        room_id_airbnb = secrets["flats"][flat_name]["rid_airbnb"]
        rate_id_airbnb = secrets["flats"][flat_name]["rtid_airbnb"]
        property_id_booking = secrets["flats"][flat_name]["pid_booking"]
        room_id_booking = secrets["flats"][flat_name]["rid_booking"]
        rate_id_booking = secrets["flats"][flat_name]["rtid_booking"]

    except KeyError:
        logging.error(f"Could not find flat name: {data['flat_name']}, is it possible you're adding columns?")
        return str("Thanks Google.")

    # Clean Date and Value
    date = pd.Timestamp(data["date"])
    value = int(data["new_value"])  # Price and min nights as integers. No real need for decimals...
    logging.info(f"Extracting data: Property: {data['flat_name']} - Date: {date.strftime('%Y-%m-%d')} - {data['value_type']}: {value}")

    # Pushing data through Zodomus:
    if data["value_type"] == "Price":
        logging.info(f"Modifying price: Pushing to channels")
        response1 = z.set_rate(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, price=value)
        response2 = z.set_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, price=value)

    logging.info("Pushed new price")
    end_time = time.time()
    logging.info(f"This took {end_time - start_time} seconds")
    app.logger.info(f"Price call took {end_time - start_time} seconds")


if __name__ == '__main__':
    app.run(debug=True)
