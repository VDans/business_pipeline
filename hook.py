import logging
import pandas as pd
from flask import Flask, request

from zodomus_api import Zodomus

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route('/availability', methods=['POST'])
def manage_availability():
    """
    Zodomus only sends a notification that a new/changed/cancelled reservation happens.
    1. Get payload from Zodomus
    2. GET /reservations with the reservation number from the webhook
    3. Using the dates and property retrieved in (.2), close dates using the API call.
    """
    data = request.json

    channel_id = data["channelId"]
    unit_id_z = data["propertyId"]
    reservation_number = data["reservationId"]

    z = Zodomus()

    if data["reservationStatus"] == '1':  # New
        logging.info("NEW booking")
        response = z.get_reservation(channel_id=channel_id, unit_id_z=unit_id_z, reservation_number=reservation_number)
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
        z.set_availability(unit_id_z=unit_id_z, date_from=date_from, date_to=date_to, availability=0)
    elif data["reservationStatus"] == '2':  # Modified
        logging.info("MODIFIED booking")
        response = z.get_reservation(channel_id=channel_id, unit_id_z=unit_id_z, reservation_number=reservation_number)
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
        z.set_availability(unit_id_z=unit_id_z, date_from=date_from, date_to=date_to, availability=1)
        z.set_availability(unit_id_z=unit_id_z, date_from=date_from, date_to=date_to, availability=0)
    elif data["reservationStatus"] == '3':  # Cancelled
        logging.info("CANCELLED booking")
        response = z.get_reservation(channel_id=channel_id, unit_id_z=unit_id_z, reservation_number=reservation_number)
        date_from = pd.Timestamp(response.json()["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response.json()["reservations"]["rooms"][0]["departureDate"])
        z.set_availability(unit_id_z=unit_id_z, date_from=date_from, date_to=date_to, availability=1)

    return str("All Good!")


if __name__ == '__main__':
    app.run(debug=True)
