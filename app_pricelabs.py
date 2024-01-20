import json
import logging
import time

from flask import Flask, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

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
