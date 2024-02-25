import json
import logging
import time

import pandas as pd
from flask import Flask, request
from channex_api import Channex
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Table, MetaData, insert, delete, types, update, select
from database_handling import DatabaseHandler
from google_api import Google


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return str("Welcome to the Web App of Host-It Immobilien GmbH")


@app.route('/new_booking', methods=['POST'])
def get_new_booking():
    """
    Channex sends a payload for a new booking.
    """
    start_time = time.time()

    data = request.json
    app.logger.info("------------------------------------------------------------------------------------------------")
    app.logger.info("NEW BOOKING ------------------------------------------------------------------------")
    if data["event"] == "booking":
        app.logger.info("Event 'booking', moving on and waiting for the payload.")
        return "OK"

    else:
        c = Channex(secrets)
        db_engine = create_engine(url=secrets["database"]["url"])
        dbh = DatabaseHandler(db_engine, secrets)
        property_id = data["payload"]["property_id"]
        revision_id = data["payload"]["booking_revision_id"]
        revision = c.get_booking_revision(revision_id)
        
        # Get Flat and Room + Consider case of several room ids:
        room_revisions_list = [r for r in revision["data"]["attributes"]["rooms"]]
        app.logger.info(f"Rooms booked: {len(room_revisions_list)}")
        for i in range(len(room_revisions_list)):
            room_id = room_revisions_list[i]["room_type_id"]
            booking_id = revision["data"]["attributes"]["ota_reservation_code"] if i == 0 else revision["data"]["attributes"]["ota_reservation_code"] + "_" + str(i + 1)
            # Process & upload booking to DB table
            dbh.upload_reservation(revision=revision, room_position=i, booking_id=booking_id)
            app.logger.info('Reservation data uploaded to table "bookings".')

            """
            # Extract dates from data:
            date_from_str = revision["data"]["attributes"]["arrival_date"]
            date_to_str = revision["data"]["attributes"]["departure_date"]
        
            # Close the dates on both platforms:
            c.update_availability_range([{
                "availability": 0,
                "date_from": date_from_str,
                "date_to": date_to_str,
                "property_id": property_id,
                "room_type_id": room_id
            }])
            logging.info("Closed availability.")
            """
        # Acknowledge the revision once all is successful.
        c.acknowledge_booking_revision(revision_id)

        end_time = time.time()
        logging.info(f"This call lasted {end_time - start_time} seconds")
        return str("All Good!")


@app.route('/modified_booking', methods=['POST'])
def get_modified_booking():
    """
    Channex sends a payload for a modified booking.
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("MODIFIED BOOKING ------------------------------------------------------------------------")

    c = Channex(secrets)
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    property_id = data["property_id"]
    revision_id = data["payload"]["revision_id"]
    revision = c.get_booking_revision(revision_id)

    # Get Flat and Room
    booking_id = ""  # ToDo: FIND RESERVATION ID!
    room_id = revision["data"]["attributes"]["rooms"]["booking_room_id"]
    flat_name = [fn for fn in secrets['flats'] if secrets["flats"][fn]["rid_channex"] == room_id][0]

    tbl = Table("bookings", MetaData(), autoload_with=db_engine)

    # Delete old booking
    with db_engine.begin() as conn:
        delete_query = delete(tbl).where(tbl.c.object == flat_name, tbl.c.status == "OK")
        conn.execute(delete_query)

        # Upload NEW reservation data to DB
        dbh.upload_reservation(revision=revision, flat_name=flat_name)
        logging.info('Reservation uploaded to table "bookings"')

        # Get OLD dates, and open them:
        old_dates = select(tbl).where(tbl.c.booking_id == booking_id)
        old_date_from = pd.Timestamp(old_dates["reservation_start"][0])
        old_date_from_str = pd.Timestamp.now().strftime("%Y-%m-%d") if old_date_from < pd.Timestamp.now() else old_date_from.strftime("%Y-%m-%d")
        old_date_to_str = pd.Timestamp(old_dates["reservation_end"][0]).strftime("%Y-%m-%d")

        # Close the dates on both platforms:
        c.update_availability_range([{
            "availability": 1,
            "date_from": old_date_from_str,
            "date_to": old_date_to_str,
            "property_id": property_id,
            "room_type_id": room_id
        }])
        logging.info("Re-opened old dates.")

        # Extract dates from data:
        new_date_from = pd.Timestamp(revision["data"]["attributes"]["arrival_date"])
        new_date_from_str = pd.Timestamp.now().strftime("%Y-%m-%d") if new_date_from < pd.Timestamp.now() else new_date_from.strftime("%Y-%m-%d")
        new_date_to_str = revision["data"]["attributes"]["departure_date"]

        # Close the dates on both platforms:
        c.update_availability_range([{
            "availability": 0,
            "date_from": new_date_from_str,
            "date_to": new_date_to_str,
            "property_id": property_id,
            "room_type_id": room_id
        }])
        logging.info("Closed new availability.")

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


@app.route('/cancelled_booking', methods=['POST'])
def get_cancelled_booking():
    """
    Channex sends a payload for a cancelled booking.
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("CANCELLED BOOKING ------------------------------------------------------------------------")

    c = Channex(secrets)
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    property_id = data["property_id"]
    revision_id = data["payload"]["revision_id"]
    revision = c.get_booking_revision(revision_id)

    # Get Flat and Room
    booking_id = ""  # ToDo: FIND RESERVATION ID!
    room_id = revision["data"]["attributes"]["rooms"]["booking_room_id"]
    flat_name = [fn for fn in secrets['flats'] if secrets["flats"][fn]["rid_channex"] == room_id][0]

    tbl = Table("bookings", MetaData(), autoload_with=db_engine)

    with db_engine.begin() as conn:
        delete_query = delete(tbl).where(tbl.c.object == flat_name, tbl.c.status == "OK")
        conn.execute(delete_query)

        # Upload updated reservation data to DB
        dbh.upload_reservation(revision=revision, flat_name=flat_name)
        logging.info('Reservation uploaded to table "bookings"')

        # Get OLD dates, and open them:
        old_dates = select(tbl).where(tbl.c.booking_id == booking_id)
        old_date_from = pd.Timestamp(old_dates["reservation_start"][0])
        old_date_from_str = pd.Timestamp.now().strftime("%Y-%m-%d") if old_date_from < pd.Timestamp.now() else old_date_from.strftime("%Y-%m-%d")
        old_date_to_str = pd.Timestamp(old_dates["reservation_end"][0]).strftime("%Y-%m-%d")

        # Close the dates on both platforms:
        c.update_availability_range([{
            "availability": 1,
            "date_from": old_date_from_str,
            "date_to": old_date_to_str,
            "property_id": property_id,
            "room_type_id": room_id
        }])
        logging.info("Re-opened old dates.")

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


@app.route('/unmapped_booking', methods=['POST'])
def handle_unmapped_booking():
    """
    Channex is not able to map this booking to a property/type/rate. Admin should be alerted.
    """

    # data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("UNMAPPED BOOKING ------------------------------------------------------------------------")

    send_email("office@host-it.at", "Error: Channex could not map booking!", "CHANNEX ERROR: Could not map booking!")

    # Create alerts
    # app.logger.warning(f"{event.title()}")
    return str("All Good!")


@app.route('/message', methods=['POST'])
def get_message():
    """
    Channex sends a notification that a message has been received.
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("NEW MESSAGE IN ------------------------------------------------------------------------")

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


def send_email(recipient_email: str, message: str, subject: str = "Check-In instructions"):
    message = Mail(
        from_email='office@host-it.at',
        to_emails=recipient_email,
        subject=subject,
        html_content=message)
    try:
        sg = SendGridAPIClient(api_key=secrets["twilio"]["email_api_key"])
        response = sg.send(message)
        logging.info(f"Email sent to {recipient_email} with response: {response.status_code}")
    except Exception as e:
        logging.error(f"Email ERROR with response: {e}")


if __name__ == '__main__':
    app.run(debug=True)
