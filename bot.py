import json
import logging
import time

import pandas as pd
from flask import Flask, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from sqlalchemy.exc import IntegrityError
from notes_horizontal import NotesH
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
from google_api import Google


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["30 per minute"]
)


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return str("Welcome to the Pricing Web App of Host-It Immobilien GmbH")


@app.route('/availability', methods=['POST'])
@limiter.exempt
def manage_availability():
    """
    Zodomus only sends a notification that a new/changed/cancelled reservation happens.
    1. Get payload from Zodomus
    2. GET /reservations with the reservation number from the webhook
    3. Using the dates and property retrieved in (.2), close dates using the API call.
    """
    start_time = time.time()  # Checking the length of each request

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("AVAILABILITY New Request------------------------------------------------------------------------")

    try:
        reservation_status_z = data["reservationStatus"]
        logging.info(f"Retrieved the reservationStatus: {reservation_status_z}")

        channel_name = "Airbnb" if str(data["channelId"]) == "3" else "Booking"

        z = Zodomus(secrets=secrets)
        g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id_horizontal"])
        db_engine = create_engine(url=secrets["database"]["url"])
        dbh = DatabaseHandler(db_engine, secrets)

        # Retrieve the reservation - Needs Zodomus Connection
        reservation_z = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"]).json()
        if str(reservation_z['status']['returnCode']) == "400":
            logging.warning(f"Status 400 was returned when looking up the reservation. Sending warning email.")
            send_email(recipient_email="office@host-it.at", subject="Reservation Error: Action Required", message=f"""Action required: Reservation not found. Data received: {data}""")
        else:
            logging.info(f"Retrieved reservation data: {data['reservationId']}")

        # Matching the flat name - Based on room_id and no more property_id
        try:
            if reservation_status_z == '3':
                # FixMe: This should consider the case where multiple rooms are cancelled for bookings which contains 2 or more rooms!
                # By cancellations, the room ID is not communicated anymore! Therefore, find the flat in the DB directly.
                flat_name_df = dbh.query_data(f"SELECT object FROM bookings WHERE status = 'OK' AND booking_id = '{data['reservationId']}'")
                flat_name = flat_name_df["object"][0]
            else:
                # ToDo: This should accept the bookings which contain more than one room booked.
                room_id = reservation_z["reservations"]["rooms"][0]["id"]
                logging.info(f"Retrieved the room_id: {room_id}")
                if str(data["channelId"]) == '1':
                    flat_name = [fn for fn in secrets['flats'] if (secrets["flats"][fn]["rid_booking"] == room_id) or (room_id in secrets["flats"][fn]["older_rid"])][0]
                elif str(data["channelId"]) == '3':
                    flat_name = [fn for fn in secrets['flats'] if (secrets["flats"][fn]["rid_airbnb"] == room_id) or (room_id in secrets["flats"][fn]["older_rid"])][0]
                else:
                    flat_name = "UNKNOWN"
                    logging.warning(f"Could not identify the channel_id: {data['channelId']}")

            logging.info(f"Retrieved the flat name: {flat_name}")

        except Exception as e:
            flat_name = "UNKNOWN"
            logging.error(f"Could NOT retrieve the flat name: {e}")

        # Initiate bookings table
        tbl = Table("bookings", MetaData(), autoload_with=db_engine)
        logging.info(f"Step 1 finishes at timestamp {time.time() - start_time} seconds.")

        if reservation_status_z == '1':  # New
            logging.info(f"New booking in {flat_name} from {reservation_z['reservations']['rooms'][0]['arrivalDate']} to {reservation_z['reservations']['rooms'][0]['departureDate']}")
            # Upload the reservation data to the DB:
            try:
                dbh.upload_reservation(channel_id_z=data["channelId"], flat_name=flat_name, reservation_z=reservation_z)
                logging.info("Reservation data uploaded to table -bookings-")
            except Exception as e:
                logging.error(f"Could NOT upload the new reservation to DB: {e}")

            # Extract dates from data:
            date_from = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["arrivalDate"])
            date_to = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["departureDate"])

            # Close the dates on both platforms:
            z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=date_from, date_to=date_to, availability=0)
            z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=date_from, date_to=date_to+pd.Timedelta(days=-1), availability=0)
            logging.info("Availability has been closed in both channels")
            logging.info(f"Step 2 finishes at timestamp {time.time() - start_time} seconds.")

        elif reservation_status_z == '2':  # Modified
            logging.info(f"Modified booking in {flat_name}")

            try:
                # Update OLD reservation data DB status to 'Modified'
                upd = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(status="Modified")
                with db_engine.begin() as conn:
                    conn.execute(upd)
                    logging.info(f"UPDATE bookings SET status = 'Modified' WHERE booking_id = '{data['reservationId']}'")

                # Upload NEW reservation data to DB
                dbh.upload_reservation(channel_id_z=data["channelId"], flat_name=flat_name, reservation_z=reservation_z)
                logging.info('Reservation uploaded to table "bookings"')

                # Get OLD dates, and open them:
                old_dates = dbh.query_data(f"SELECT reservation_start, reservation_end FROM bookings WHERE status = 'Modified' AND booking_id = '{data['reservationId']}'")
                old_date_from = pd.Timestamp(old_dates["reservation_start"][0])
                old_date_to = pd.Timestamp(old_dates["reservation_end"][0])

                # Safety. If the from_date is before today, it would refuse to close!
                old_date_from_today = pd.Timestamp.now() if old_date_from < pd.Timestamp.now() else old_date_from
                z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=old_date_from_today, date_to=old_date_to, availability=1)
                z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=old_date_from_today, date_to=old_date_to+pd.Timedelta(days=-1), availability=1)
                logging.info("Old dates have been opened in both channels")

                # Get NEW dates, and close them:
                new_date_from = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["arrivalDate"])
                new_date_to = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["departureDate"])

                # Safety. If the from_date is before today, it would refuse to close!
                new_date_from_today = pd.Timestamp.now() if new_date_from < pd.Timestamp.now() else new_date_from
                z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=new_date_from_today, date_to=new_date_to, availability=0)
                z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=new_date_from_today, date_to=new_date_to+pd.Timedelta(days=-1), availability=0)
                logging.info(f"New dates have been closed in both channels: {new_date_from.strftime('%Y-%m-%d')} to {new_date_to.strftime('%Y-%m-%d')}")
                logging.info(f"Step 2 finishes at timestamp {time.time() - start_time} seconds.")

            except KeyError as ke:
                logging.error(f"ERROR in the processing of the modification: {ke}")

        elif reservation_status_z == '3':  # Cancelled
            logging.info(f"Cancelled booking {str(data['reservationId'])} in {flat_name}")
            try:
                with db_engine.begin() as conn:
                    try:
                        # Set cleaning fee to 0
                        upd2 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(cleaning=0)
                        conn.execute(upd2)
                        logging.info(f"UPDATE bookings SET cleaning = 0 WHERE booking_id = '{data['reservationId']}'")
                    except Exception as ex:
                        logging.error(f"Couldn't update cleaning fee: {ex}")

                    try:
                        # Fetch the updated reservation price
                        upd3 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(nights_price=float(reservation_z['reservations']['reservation']['totalPrice'].replace(",", "")))
                        # Update the platform commission
                        r_com = -0.15 if str(data["channelId"]) == '3' else -0.162
                        upd4 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(commission_host=float(reservation_z['reservations']['reservation']['totalPrice'].replace(",", "")) * r_com)
                    except Exception as ex:
                        upd3 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(nights_price=0)
                        upd4 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(commission_host=0)
                        logging.error(f"Couldn't update prices: {ex}")
                    finally:
                        conn.execute(upd3)
                        logging.info(f"UPDATE bookings SET nights_price = {reservation_z['reservations']['reservation']['totalPrice']} WHERE booking_id = '{data['reservationId']}'")
                        conn.execute(upd4)
                        logging.info(f"UPDATE bookings SET commission_host = 15% nets WHERE booking_id = '{data['reservationId']}'")

                    try:
                        # Set Status to Cancelled
                        upd1 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(status="Cancelled")
                        conn.execute(upd1)
                        logging.info(f"UPDATE bookings SET status = 'Cancelled' WHERE booking_id = '{data['reservationId']}'")
                    except Exception as ex:
                        logging.error(f"Couldn't update status: {ex}")

                # Have to get the dates from the DB because not provided by
                dates = dbh.query_data(f"SELECT reservation_start, reservation_end FROM bookings WHERE booking_id = '{data['reservationId']}'")
                date_from = pd.Timestamp(dates["reservation_start"][0])
                date_to = pd.Timestamp(dates["reservation_end"][0])

                z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=date_from, date_to=date_to, availability=1)
                z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=date_from, date_to=date_to+pd.Timedelta(days=-1), availability=1)
                logging.info("Availability has been opened in both channels")

            except KeyError as ke:
                logging.error(f"ERROR in the processing of the cancellation: {ke}")

        else:
            logging.error(f"reservationStatus not understood: {reservation_status_z}")

        # Rewrite all sheet, whatever happened:
        # n = NotesH(secrets=secrets, google=g)
        # n.write_notes()

        dbh.close_engine()

    except Exception as e:
        logging.error(f"Couldn't find the 'ReservationStatus' in the request: {e}")
        logging.info(f"The request says: {data}")

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


@app.route('/online-checkin', methods=['POST'])
@limiter.exempt
def check_in_online():
    """
    Receive webhook from TypeForm Website with needed data.
    This call also triggers the expedition of the check-in instructions to the
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("OCI New Request---------------------------------------------------------------------------------")

    fa = data["form_response"]["answers"]
    logging.info(f"New online check-in submitted. Uploading to DB...")

    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    # The first step is to identify the booking.
    try:
        booking_id = data["form_response"]["hidden"]["booking_id"]
        if booking_id == "":
            booking_id_json = list(filter(lambda x: x["field"]["id"] == "aYx6wPeuUVQB", fa))
            booking_id = booking_id_json[0]["text"].upper()

    except Exception as e:
        booking_id = None
        logging.error(f"Could not find booking_id with error: {e}")

    try:
        complete_name_json = list(filter(lambda x: x["field"]["id"] == "2jL0fJRRhvIx", fa))
        complete_name = complete_name_json[0]["text"].title()
    except Exception as e:
        complete_name = None
        logging.error(f"Could not find Complete Name with error: {e}")

    try:
        birth_date_json = list(filter(lambda x: x["field"]["id"] == "a0yIBjOxHLpz", fa))
        birth_date = pd.Timestamp(birth_date_json[0]["date"])
    except Exception as e:
        birth_date = pd.Timestamp(day=1, month=1, year=2050)
        logging.error(f"Could not find Birth Date with error: {e}")

    try:
        nationality_json = list(filter(lambda x: x["field"]["id"] == "nG5ElmuMaUjI", fa))
        nationality = nationality_json[0]["choice"]["label"]
    except Exception as e:
        nationality = None
        logging.error(f"Could not find Nationality with error: {e}")

    try:
        address1_json = list(filter(lambda x: x["field"]["id"] == "iyagz93Mh58C", fa))

        try:
            address2_json = list(filter(lambda x: x["field"]["id"] == "tHxUHfdhaCXb", fa))
            address2 = address2_json[0]['text']
        except Exception as e:
            logging.info(f"Guest did not enter address line 2: {e}")
            address2 = ""

        address3_json = list(filter(lambda x: x["field"]["id"] == "ezg8z74NJOAv", fa))
        address4_json = list(filter(lambda x: x["field"]["id"] == "QmIqIUzZNNX0", fa))
        address5_json = list(filter(lambda x: x["field"]["id"] == "eSle7zYGph6M", fa))
        address = f"{address1_json[0]['text']} {address2} {address3_json[0]['text']} {address4_json[0]['text']} {address5_json[0]['text']}"
    except Exception as e:
        address = None
        logging.error(f"Could not find Address with error: {e}")

    try:
        country_json = list(filter(lambda x: x["field"]["id"] == "4nOmw728gGvk", fa))
        country = country_json[0]["text"]
    except Exception as e:
        country = None
        logging.error(f"Could not find country with error: {e}")

    try:
        email_address_json = list(filter(lambda x: x["field"]["id"] == "Ldc3ijn1LoNl", fa))
        email_address = email_address_json[0]["email"]
    except Exception as e:
        email_address = None
        logging.error(f"Could not find email_address with error: {e}")

    try:
        phone_number_json = list(filter(lambda x: x["field"]["id"] == "jLITNsgAaEzd", fa))
        phone_number = phone_number_json[0]["phone_number"].replace(" ", "")
    except Exception as e:
        phone_number = None
        logging.error(f"Could not find phone_number with error: {e}")

    try:
        id_type_json = list(filter(lambda x: x["field"]["id"] == "ju5jsKP93xpa", fa))
        id_type = id_type_json[0]["choice"]["label"]
    except Exception as e:
        id_type = None
        logging.error(f"Could not find id_type with error: {e}")

    try:
        eta_json = list(filter(lambda x: x["field"]["id"] == "wyC1teWTVx6P", fa))
        eta = eta_json[0]["choice"]["label"]
    except Exception as e:
        eta = None
        logging.error(f"Could not find ETA with error: {e}")

    try:
        eci_json = list(filter(lambda x: x["field"]["id"] == "d5KeUxBhRENE", fa))
        eci_payment_success = eci_json[0]["payment"]["success"]
        if eci_payment_success:
            logging.info("Early check-in has been confirmed.")
            eta = "PRIORITÄT, so früh wie möglich, Max. 13:00."
            send_email(recipient_email="office@host-it.at",
                       message=f"New early check-in booked!\nGuest Name: {complete_name}\nBooking ID: {booking_id}",
                       subject="New early check-in booked!")
    except Exception as e:
        eci_payment_success = None
        logging.error(f"Could not find ETA with error: {e}")

    try:
        etd_json = list(filter(lambda x: x["field"]["id"] == "z4d9kcjjt2ik", fa))
        etd = etd_json[0]["choice"]["label"]
    except Exception as e:
        etd = None
        logging.error(f"Could not find ETD with error: {e}")

    try:
        beds_json = list(filter(lambda x: x["field"]["id"] == "dGapMiuCAuWX", fa))
        beds = beds_json[0]["text"]
    except Exception as e:
        beds = None
        logging.error(f"Guest did not enter beds preference: {e}")

    out = pd.DataFrame({
        "complete_name": [complete_name],
        "birth_date": [birth_date],
        "nationality": [nationality],
        "address": [address],
        "country": [country],
        "email_address": [email_address],
        "phone_number": [phone_number],
        "id_type": [id_type],
        "eta": [eta],
        "etd": [etd],
        "beds": [beds],
        "booking_id": [booking_id],
        "submission_date": [pd.Timestamp.now()],
        "eci": eci_payment_success
    })

    logging.info(f"Checking if this combination of booking_id and complete name is already on the database...")
    dbh.query_data(sql=f"SELECT * FROM checkin_data WHERE booking_id = '{booking_id}' AND complete_name = '{complete_name}';")

    try:
        out.to_sql(name="checkin_data",
                   con=db_engine,
                   if_exists="append",
                   index=False)
    except IntegrityError as ie:
        logging.error(f"A duplicate has been found, and the new data was not uploaded: {ie}")

    logging.info(f"Data uploaded to the DB with success: {complete_name}")

    # Find email corresponding to the booking_id:
    try:
        _out = dbh.query_data(sql=f"""SELECT email, object FROM bookings WHERE booking_id = '{booking_id}'""")

        if len(_out) > 0:
            # Get the language:
            language = 'german' if country in ['Austria', 'Germany'] else 'english'

            # Get the flat name:
            flat_name = _out["object"][0]

            # Get the email:
            recipient_email = _out["email"][0]

            # Get the message & language:
            _check_in_instructions = dbh.query_data(sql=f"""SELECT message FROM messages WHERE message_language = '{language}' and flat_name = '{flat_name}'""")
            if len(_check_in_instructions) > 0:
                check_in_instructions = _check_in_instructions["message"][0]
                logging.info(f"Checking instructions in {language} for flat {flat_name} found.")

                # Send the emails - 1 to the platform-bound email address, one to the email address given by the guest:
                send_email(recipient_email=recipient_email, message=check_in_instructions)
                send_email(recipient_email=email_address, message=check_in_instructions)

            else:
                logging.info(f"Checking instructions in {language} for flat {flat_name} NOT found. Switching language...")
                language = 'english' if language == 'german' else 'german'
                _check_in_instructions = dbh.query_data(sql=f"""SELECT message FROM messages WHERE message_language = '{language}' and flat_name = '{flat_name}'""")

                if len(_check_in_instructions) > 0:
                    check_in_instructions = _check_in_instructions["message"][0]
                    logging.info(f"Checking instructions in {language} for flat {flat_name} found.")

                    # Send the emails - 1 to the platform-bound email address, one to the email address given by the guest:
                    send_email(recipient_email=recipient_email, message=check_in_instructions)
                    send_email(recipient_email=email_address, message=check_in_instructions)

                else:
                    logging.info(f"Checking instructions in {language} for flat {flat_name} NOT found. The instructions are not available in this flat...")

        else:
            send_email(recipient_email="office@host-it.at", subject="Online Check-In Warning", message=f"The guest {complete_name} has given the comfirmation number {booking_id}, which hasn't been found in the database.\nTherefore, they have not received any instructions!\nPlease make sure the data is right, and send manually.")
            logging.warning(f"Could NOT find the booking_id {booking_id} given by the guest! Could it be from a property outside of the system")

    except Exception as e:
        logging.error(f"Could NOT even reach the querying using this booking_id: {e}")

    dbh.close_engine()

    end_time = time.time()
    logging.info(f"This took {end_time - start_time} seconds")

    return str("Thanks for checking in!")


@app.route('/code', methods=['POST'])
@limiter.exempt
def get_code():
    """
    This url is called by the Google Webhook when a change occurs in the Lockboxes Codes' Google Sheet.

    THIS IS SHIT AND NOT WORKING. FIX.
    """
    start_time = time.time()

    data = request.json
    logging.info("--------------------------------------------------------------------------------------------------------")
    logging.info("LOCKBOX CODES New Request-------------------------------------------------------------------------------------")

    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    tbl = Table("entry_codes", MetaData(), autoload_with=db_engine)

    flat_name = data["flat_name"]
    new_code = data["new_code"]
    timestamp_now = pd.Timestamp.now()

    try:
        existing_flat = dbh.query_data(f"SELECT flat_name FROM entry_codes WHERE flat_name = '{flat_name}'")
        if len(existing_flat["flat_name"]) == 0:
            out = pd.DataFrame([{
                "flat_name": flat_name,
                "last_changed": timestamp_now,
                "code": new_code
            }])
            logging.warning(f"Flat NOT found on the database. Adding new row with new code...")
            out.to_sql(
                index=False,
                con=db_engine,
                name='entry_codes',
                if_exists='append'
            )
        else:
            logging.warning(f"Flat found on the database. Changing code...")
            try:
                # Update code on DB to new code
                upd = update(tbl).where(tbl.c.flat_name == flat_name).values(code=new_code)
                with db_engine.begin() as conn:
                    conn.execute(upd)
                    logging.info(f"UPDATE entry_codes SET code = '{new_code}' WHERE flat_name = '{flat_name}'")
            except Exception as ex:
                logging.error(f"Could not update code on existing flat: {ex}")

            try:
                # Update Timestamp on DB to current time
                upd = update(tbl).where(tbl.c.flat_name == flat_name).values(last_changed=timestamp_now)
                with db_engine.begin() as conn:
                    conn.execute(upd)
                    logging.info(f"UPDATE entry_codes SET last_changed = '{timestamp_now}' WHERE flat_name = '{flat_name}'")
            except Exception as ex:
                logging.error(f"Could not update timestamp on existing flat: {ex}")

    except Exception as ex:
        logging.error(f"Could not add code: {ex}")

    logging.info(f"This took {time.time() - start_time} seconds")
    return str("Changed Code (Supposedly)")


def get_n_guests(reservation_z):
    """Get number of guests from shitty reservation format"""
    data = reservation_z["reservations"]
    # The way n_adults and n_children are written is shameful in the API...
    adults = 0
    children = 0
    guests = data["rooms"][0]["guestCount"]  # List of dicts
    for g in guests:
        if g["adult"] == 1:
            adults += int(g["count"])
        else:
            children += int(g["count"])
    return adults + children


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


def add_write_snippet(booking_date, google, data, flat, value):
    cell_range = google.get_rolling_range(unit_id=flat, date1=booking_date, col=secrets["flats"][flat]["pricing_col"], headers_rows=3)
    snippet = {
        "range": cell_range,
        "values": [
            [value]
        ]
    }
    data.append(snippet)


def add_write_snippet_h(booking_date, google, data, flat, value):
    # Calculate the A1 notation of where the name of the booking should be.
    # In this new concept, the name should expand on two rows.
    target_col = google.get_rolling_col(date1=booking_date, today_col="L")
    snippet = {
        "range": target_col + str(secrets["flats"][flat]["pricing_row"]),
        "values": [
            [value]
        ]
    }
    data.append(snippet)


if __name__ == '__main__':
    app.run()
