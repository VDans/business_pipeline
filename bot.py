import json
import logging
import time
import pandas as pd
from flask import Flask, request
from twilio.rest import Client

from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))


@app.route('/', methods=['POST'])
def hello_world():
    return str("Hello world!")


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
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    flat_name = secrets["flat_names"][data["propertyId"]]  # Flat common name

    if data["reservationStatus"] == '1':  # New
        logging.info(f"New booking in {flat_name}")

        response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"])
        logging.info(json.dumps(response.json(), indent=3))
        dbh.upload_reservation(channel_id_z=data["channelId"], unit_id_z=data["propertyId"], reservation_z=response)
        logging.info("Reservation uploaded to table -bookings-")
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])

        z.set_availability(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to, availability=0)
        z.set_availability(channel_id="3", unit_id_z=secrets["airbnb"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["airbnb"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to, availability=0)
        logging.info("Availability has been closed in both channels")

        body = f"New Booking in {flat_name}:\n{date_from} to {date_to}\n"

    # elif data["reservationStatus"] == '2':  # Modified
    #     logging.info("MODIFIED booking")
    #     response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"])
    #     date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
    #     date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
    #     # ToDo: Find "old" dates and open them.
    #     # z.set_availability(unit_id_z=data["propertyId"], date_from=date_from, date_to=date_to, availability=1)
    #     z.set_availability(unit_id_z=data["propertyId"], date_from=date_from, date_to=date_to, availability=0)
    #
    #     body = f"Modified Booking:\n{response.json()['reservations']['rooms'][0]['arrivalDate']} to {response.json()['reservations']['rooms'][0]['departureDate']}\n"

    elif data["reservationStatus"] == '3':  # Cancelled
        logging.info(f"Cancelled booking in {flat_name}")

        response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"])
        logging.info(json.dumps(response.json(), indent=3))



        z.set_availability(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to, availability=1)
        # z.set_availability(channel_id="3", unit_id_z=secrets["airbnb"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["airbnb"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to, availability=1)

        body = f"Cancelled Booking in {flat_name}:\n{response.json()['reservations']['rooms'][0]['arrivalDate']} to {response.json()['reservations']['rooms'][0]['departureDate']}\n"

    else:
        logging.error(f"reservationStatus not understood: {data['reservationStatus']}")
        body = f"reservationStatus not understood: {data['reservationStatus']}"

    # # Send response to Valentin by Whatsapp Message:
    # client = Client(secrets['twilio']['account_sid'], secrets['twilio']['auth_token'])
    # client.messages.create(from_="whatsapp:+436703085269",
    #                        to=f"whatsapp:+436601644192",
    #                        body=body)

    return str("All Good!")


@app.route('/pricing', methods=['POST'])
def get_prices():
    """
    This url is called by the Google Webhook when a change occurs in the pricing Google Sheet.
    """
    data = request.json

    z = Zodomus(secrets=secrets)

    # Find out the booking and airbnb propertyId
    property_id_airbnb = secrets["airbnb"]["flat_ids"][data["flat_name"]]["propertyId"]
    room_id_airbnb = secrets["airbnb"]["flat_ids"][data["flat_name"]]["roomId"]
    rate_id_airbnb = secrets["airbnb"]["flat_ids"][data["flat_name"]]["rateId"]
    property_id_booking = secrets["booking"]["flat_ids"][data["flat_name"]]["propertyId"]
    room_id_booking = secrets["booking"]["flat_ids"][data["flat_name"]]["roomId"]
    rate_id_booking = secrets["booking"]["flat_ids"][data["flat_name"]]["rateId"]

    for i in range(len(data["new_value"])):
        # Clean Date and Value
        date = pd.Timestamp(data["date"][i][0])
        value = int(data["new_value"][i][0])  # Price and min nights as integers. No real need for decimals...

        logging.info(f"Extracting data: \nProperty: {data['flat_name']}\nDate: {date.strftime('%Y-%m-%d')}\n{data['value_type']}: {value} \n---------------")

        # Quick Checks of data before sending
        value_check(value_type=data['value_type'], value=value)

        # Pushing data through Zodomus:
        # if data["value_type"] == "Price":
        #     response1 = z.set_rate(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, price=value)
        #     response2 = z.set_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, price=value)
        #
        # elif data["value_type"] == "Min.":
        #     response1 = z.set_minimum_nights(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, min_nights=value)
        #     response2 = z.set_minimum_nights(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, min_nights=value)
        #
        # else:
        #     response1 = response2 = "value_type data not one of 'Price' or 'Min.'"
        #
        # logging.info(response1)
        # logging.info(response2)

    return str("Thanks Google!")


def value_check(value_type, value):
    if value_type == "Price":
        # Empty values:
        if value == '':
            value = 0

        # Reasonable prices between 20 and 2000EUR. If not, alert.
        if 20 > value > 1100:
            logging.warning("The Price is below 20 or above 2000EUR! Is this right?")
        else:
            logging.info("Price passed reasonable check.")

    elif value_type == "Min.":
        # Reasonable min_nights between 1 and 7. If not, alert.
        if 1 > value > 7:
            logging.warning("The min_night parameter is below 1 or above 7! Is this right?")
        else:
            logging.info("min_night parameter passed reasonable check.")

    return value


if __name__ == '__main__':
    app.run(debug=True)
