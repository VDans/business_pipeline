import json
import logging
import pandas as pd
from flask import Flask, request
from twilio.rest import Client

from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
from google_api import Google

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
    logging.info("\nNew Request-------------------------------------------------------------------------------------")

    z = Zodomus(secrets=secrets)
    g = Google(secrets=secrets)
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    flat_name = secrets["flat_names"][data["propertyId"]]  # Flat common name
    channel_name = "Airbnb" if str(data["channelId"]) == "3" else "Booking.com"

    if data["reservationStatus"] == '1':  # New
        logging.info(f"New booking in {flat_name}")

        response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"]).json()
        logging.info("Retrieved reservation data")
        dbh.upload_reservation(channel_id_z=data["channelId"], unit_id_z=data["propertyId"], reservation_z=response)
        logging.info("Reservation uploaded to table -bookings-")
        date_from = pd.Timestamp(response["reservations"]["rooms"][0]["arrivalDate"])
        date_to = pd.Timestamp(response["reservations"]["rooms"][0]["departureDate"])

        z.set_availability(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to, availability=0)
        z.set_availability(channel_id="3", unit_id_z=secrets["airbnb"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["airbnb"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to+pd.Timedelta(days=-1), availability=0)
        logging.info("Availability has been closed in both channels")

        # Write the "Booked" in the Google Sheet
        dates1 = list(pd.date_range(start=date_from, end=(date_to+pd.Timedelta(days=-1))))
        for d in dates1:
            cell_range = g.get_pricing_range(unit_id=flat_name,
                                             date1=d)
            response1 = g.write_to_cell(cell_range, value=channel_name)
        logging.info(f"Wrote '{channel_name}' within the pricing Google Sheet")

        body = f"""You have received a new reservation in {flat_name}\nName: {response['reservations']['customer']['firstName'] + ' ' + response['reservations']['customer']['lastName']}\nDates: {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}\nPrice: {str(response['reservations']['rooms'][0]['totalPrice'])}"""

    elif data["reservationStatus"] == '2':  # Modified
        logging.info(f"Modified booking in {flat_name}")

        try:
            # Get NEW reservation data:
            response = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"]).json()
            logging.info("Retrieved reservation data")

            # Update OLD reservation data DB status to 'Modified'
            dbh.curs.execute(f"UPDATE bookings SET status = 'Modified' WHERE booking_id = '{data['reservationId']}'")
            logging.info(f"UPDATE bookings SET status = 'Modified' WHERE booking_id = '{data['reservationId']}'")

            # Upload NEW reservation data to DB
            dbh.upload_reservation(channel_id_z=data["channelId"], unit_id_z=data["propertyId"], reservation_z=response)
            logging.info("Reservation uploaded to table -bookings-")

            # Get OLD dates, and open them:
            old_dates = dbh.query_data(f"SELECT reservation_start, reservation_end FROM bookings WHERE status = 'Modified' AND booking_id = '{data['reservationId']}'")
            old_date_from = pd.Timestamp(old_dates["reservation_start"][0])
            old_date_to = pd.Timestamp(old_dates["reservation_end"][0])
            z.set_availability(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][flat_name]["roomId"], date_from=old_date_from, date_to=old_date_to, availability=1)
            z.set_availability(channel_id="3", unit_id_z=secrets["airbnb"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["airbnb"]["flat_ids"][flat_name]["roomId"], date_from=old_date_from, date_to=old_date_to+pd.Timedelta(days=-1), availability=1)
            logging.info("Old dates have been opened in both channels")

            # Get NEW dates, and close them:
            new_date_from = pd.Timestamp(response["reservations"]["rooms"][0]["arrivalDate"])
            new_date_to = pd.Timestamp(response["reservations"]["rooms"][0]["departureDate"])
            z.set_availability(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][flat_name]["roomId"], date_from=new_date_from, date_to=new_date_to, availability=0)
            z.set_availability(channel_id="3", unit_id_z=secrets["airbnb"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["airbnb"]["flat_ids"][flat_name]["roomId"], date_from=new_date_from, date_to=new_date_to+pd.Timedelta(days=-1), availability=0)
            logging.info("New dates have been closed in both channels")

            # Remove the "Booked" in the Google Sheet
            dates1 = list(pd.date_range(start=old_date_from, end=(old_date_to+pd.Timedelta(days=-1))))
            for d in dates1:
                cell_range = g.get_pricing_range(unit_id=flat_name,
                                                 date1=d)
                response1 = g.write_to_cell(cell_range, value=4)
            logging.info("Remove the 'Booked' tag within the pricing Google Sheet")

            # Write the "Booked" in the Google Sheet
            dates2 = list(pd.date_range(start=new_date_from, end=(new_date_to+pd.Timedelta(days=-1))))
            for d in dates2:
                cell_range = g.get_pricing_range(unit_id=flat_name,
                                                 date1=d)
                response1 = g.write_to_cell(cell_range, value=channel_name)
            logging.info(f"Wrote '{channel_name}' within the pricing Google Sheet")

            body = f"Modified Booking in {flat_name}:\n{new_date_from.strftime('%Y-%m-%d')} to {new_date_to.strftime('%Y-%m-%d')}\n"

        except KeyError as ke:
            logging.error(f"ERROR in the processing of the modification: {ke}")
            body = f"ERROR: Could not process modification"

    elif data["reservationStatus"] == '3':  # Cancelled
        logging.info(f"Cancelled booking in {flat_name}")

        try:
            dbh.curs.execute(f"UPDATE bookings SET status = 'Cancelled' WHERE booking_id = '{data['reservationId']}'")
            logging.info(f"UPDATE bookings SET status = 'Cancelled' WHERE booking_id = '{data['reservationId']}'")
            dates = dbh.query_data(f"SELECT reservation_start, reservation_end FROM bookings WHERE booking_id = '{data['reservationId']}'")
            date_from = pd.Timestamp(dates["reservation_start"][0])
            date_to = pd.Timestamp(dates["reservation_end"][0])

            z.set_availability(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to, availability=1)
            z.set_availability(channel_id="3", unit_id_z=secrets["airbnb"]["flat_ids"][flat_name]["propertyId"], room_id_z=secrets["airbnb"]["flat_ids"][flat_name]["roomId"], date_from=date_from, date_to=date_to+pd.Timedelta(days=-1), availability=1)
            logging.info("Availability has been opened in both channels")

            # Remove the "Booked" in the Google Sheet
            dates1 = list(pd.date_range(start=date_from, end=(date_to+pd.Timedelta(days=-1))))
            for d in dates1:
                cell_range = g.get_pricing_range(unit_id=flat_name,
                                                 date1=d)
                response1 = g.write_to_cell(cell_range, value=4)
            logging.info("Remove the 'Booked' tag within the pricing Google Sheet")

            body = f"Cancelled Booking in {flat_name}:\n{date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}\n"

        except KeyError as ke:
            logging.error(f"ERROR in the processing of the cancellation: {ke}")
            body = f"ERROR: Could not process cancellation"

    else:
        logging.error(f"reservationStatus not understood: {data['reservationStatus']}")
        body = f"reservationStatus not understood: {data['reservationStatus']}"

    # Send response by Whatsapp Message:
    client = Client(secrets['twilio']['account_sid'], secrets['twilio']['auth_token'])
    for n in [secrets['twilio']['whatsapp_valentin']]:  #, secrets['twilio']['whatsapp_ilian']]:
        client.messages.create(from_="whatsapp:+436703085269",
                               to=n,
                               body=body)

    dbh.close_engine()

    return str("All Good!")


@app.route('/pricing', methods=['POST'])
def get_prices():
    """
    This url is called by the Google Webhook when a change occurs in the pricing Google Sheet.
    """
    data = request.json
    logging.info("\nNew Request-------------------------------------------------------------------------------------")

    z = Zodomus(secrets=secrets)

    # Find out the booking and airbnb propertyId
    property_id_airbnb = secrets["airbnb"]["flat_ids"][data["flat_name"]]["propertyId"]
    room_id_airbnb = secrets["airbnb"]["flat_ids"][data["flat_name"]]["roomId"]
    rate_id_airbnb = secrets["airbnb"]["flat_ids"][data["flat_name"]]["rateId"]
    property_id_booking = secrets["booking"]["flat_ids"][data["flat_name"]]["propertyId"]
    room_id_booking = secrets["booking"]["flat_ids"][data["flat_name"]]["roomId"]
    rate_id_booking = secrets["booking"]["flat_ids"][data["flat_name"]]["rateId"]

    for i in range(len(data["new_value"])):

        try:
            # Clean Date and Value
            date = pd.Timestamp(data["date"][i][0])
            value = int(data["new_value"][i][0])  # Price and min nights as integers. No real need for decimals...

            logging.info(f"Extracting data: \nProperty: {data['flat_name']}\nDate: {date.strftime('%Y-%m-%d')}\n{data['value_type']}: {value} \n---------------")

            # Pushing data through Zodomus:
            if data["value_type"] == "Price":
                response1 = z.set_rate(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, price=value)
                logging.info(f"Booking response: {response1.json()['status']['returnMessage']}")
                response2 = z.set_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, price=value)
                logging.info(f"Airbnb response: {response2.json()['status']['returnMessage']}")

            elif data["value_type"] == "Min.":

                # UNFORTUNATELY the shitty Airbnb API requires a price push at the same time as the minimum nights' push.
                # Therefore, you also have to communicate the price next to the min nights requirements...
                rightCellValue = int(data["rightCellValue"])

                # 1. Make sure the dates are open:
                response0 = z.set_availability(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, date_from=date, date_to=(date + pd.Timedelta(days=1)), availability=1)
                logging.info(f"Booking response: {response0.json()['status']['returnMessage']}")
                response0 = z.set_availability(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, date_from=date, date_to=date, availability=1)
                logging.info(f"Airbnb response: {response0.json()['status']['returnMessage']}")

                # 2. Change the minimum nights on the platforms
                response1 = z.set_minimum_nights(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, min_nights=value)
                logging.info(f"Booking response: {response1.json()['status']['returnMessage']}")
                response2 = z.set_airbnb_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, price=rightCellValue, min_nights=value)  # Fucking hate this...
                logging.info(f"Airbnb response: {response2.json()['status']['returnMessage']}")

                if str(value) == "0":
                    # 3. If min_nights = 0: Close the room for the night in both channels
                    logging.info("Min. Nights set to 0. Closing the room.")
                    response3 = z.set_availability(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, date_from=date, date_to=(date + pd.Timedelta(days=1)), availability=0)
                    logging.info(f"Booking response: {response3.json()['status']['returnMessage']}")
                    response4 = z.set_availability(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, date_from=date, date_to=date, availability=0)
                    logging.info(f"Airbnb response: {response4.json()['status']['returnMessage']}")

            else:
                response1 = response2 = "value_type data not one of 'Price' or 'Min.'"
                logging.warning(f"Response: {response1}")

        except ValueError:
            if data["new_value"][i][0] in ["Booked", "Airbnb", "Booking.com"]:
                logging.warning(f"New '{data['new_value'][i][0]}' value entered. Skipping the logic.")
            else:
                logging.warning(f"Value entered is not a valid integer. Skipping the logic.")

    return str("Thanks Google!")


if __name__ == '__main__':
    app.run(debug=True)
