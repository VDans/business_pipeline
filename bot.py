import json
import logging
import time
import pandas as pd
from flask import Flask, request
from twilio.rest import Client

from zodomus_api import Zodomus

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))


@app.route('/availability', methods=['POST'])
def manage_availability():
    """
    Zodomus only sends a notification that a new/changed/cancelled reservation happens.
    1. Get payload from Zodomus
    2. GET /reservations with the reservation number from the webhook
    3. Using the dates and property retrieved in (.2), close dates using the API call.
    """
    data = request.json

    z = Zodomus(secrets=secrets)

    if data["reservationStatus"] == '1':  # New
        logging.info("NEW booking")
        response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"])
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
        z.set_availability(unit_id_z=data["propertyId"], date_from=date_from, date_to=date_to, availability=0)

        body = f"New Booking:\n{response.json()['reservations']['rooms'][0]['arrivalDate']} to {response.json()['reservations']['rooms'][0]['departureDate']}\n"

    elif data["reservationStatus"] == '2':  # Modified
        logging.info("MODIFIED booking")
        response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"])
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
        # ToDo: Find "old" dates and open them.
        # z.set_availability(unit_id_z=data["propertyId"], date_from=date_from, date_to=date_to, availability=1)
        z.set_availability(unit_id_z=data["propertyId"], date_from=date_from, date_to=date_to, availability=0)

        body = f"Modified Booking:\n{response.json()['reservations']['rooms'][0]['arrivalDate']} to {response.json()['reservations']['rooms'][0]['departureDate']}\n"

    elif data["reservationStatus"] == '3':  # Cancelled
        logging.info("CANCELLED booking")
        response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"])
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
        z.set_availability(unit_id_z=data["propertyId"], date_from=date_from, date_to=date_to, availability=1)

        body = f"Cancelled Booking:\n{response.json()['reservations']['rooms'][0]['arrivalDate']} to {response.json()['reservations']['rooms'][0]['departureDate']}\n"

    else:
        body = "reservationStatus not 1, 2 or 3."

    # Send response to Valentin by Whatsapp Message:
    client = Client(secrets['twilio']['account_sid'], secrets['twilio']['auth_token'])
    client.messages.create(from_="whatsapp:+436703085269",
                           to=f"whatsapp:+436601644192",
                           body=body)

    return str("All Good!")


@app.route('/pricing', methods=['POST'])
def get_prices():
    """
    This url is called by the Google Webhook when a change occurs in the pricing Google Sheet.
    """

    data = request.json
    time.sleep(1)  # Give the code some time between the calls.

    # update_prices()
    # update_min_nights()

    return str("Thanks Google!")


if __name__ == '__main__':
    app.run()
