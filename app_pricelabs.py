import json
import logging
import time
import pandas as pd
from datetime import datetime
from flask import Flask, request
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
    NEW IN DEVELOPMENT: This endpoint also arrives from Google, but is now meant to update prices on the DB table, no more to push on the platform.
    The prices will ideally get pushed with a confirmation button, in "batches" instead of individual prices.
    This call then uploads to the DB the new changes.
    A task then transmits the DB prices to the platforms.
    """
    start_time = time.time()

    data = request.json
    logging.info("--------------------------------------------------------------------------------------------------------")
    logging.info("PRICING New Request-------------------------------------------------------------------------------------")

    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)
    pricing_tbl = Table("pricing", MetaData(), autoload_with=db_engine)

    # First slow but easy method:
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

    # Filter after today
    df_google["price_date"] = df_google["price_date"][df_google["price_date"] >= datetime.today().date()]

    # Filter rows where min_nights values are not numbers. They are bookings.
    df_google["min_nights"] = pd.to_numeric(df_google["min_nights"], errors="coerce")
    df_google.dropna(how="any", inplace=True)

    # Now prepare the database table:
    df_db = dbh.query_data(f"SELECT * FROM pricing WHERE price_date >= CURRENT_DATE")

    # Add the new rows. If two rows are the same price date and object, keep the Google one (latest!):
    df_diff = pd.concat([df_google, df_db], ignore_index=True).drop_duplicates(subset=["price_date", "object", "price", "min_nights"], keep=False)
    # Only the new changes remain. Now only keep the latest changed value:
    df_diff = df_diff.drop_duplicates(subset=["price_date", "object"], keep="first").reset_index(drop=True)

    print(df_diff)

    # Update is not a viable solution, as sometimes you need to add rows with new dates!
    # Delete where the index matches:
    if len(df_diff) > 0:
        with db_engine.begin() as conn:
            for i in range(len(df_diff)):
                p_d = df_diff["price_date"][i]
                o = df_diff["object"][i]
                delete_query = delete(pricing_tbl).where(pricing_tbl.c.object == o, pricing_tbl.c.price_date == p_d)
                conn.execute(delete_query)
                logging.info(f"Replacing pricing where price_date = {p_d} and object = {o}")

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


if __name__ == '__main__':
    app.run(debug=True)
