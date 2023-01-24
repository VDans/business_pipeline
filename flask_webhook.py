import pandas as pd
import logging
from flask import Flask, request, json, render_template
from sqlalchemy import create_engine

from Messaging.twilio_sms import SmsEngine
from Messaging.twilio_whatsapp import Whatsapp
from Platforms.smoobu import Smoobu

logging.basicConfig(filename='error.log', level=logging.INFO)


try:
    secrets = json.load(open('/etc/config_secrets.json'))
    resources = json.load(open('Databases/resources_help.json'))
except FileNotFoundError:
    secrets = json.load(open('config_secrets.json'))
    resources = json.load(open('Databases/resources_help.json'))

db_engine = create_engine(url=secrets['database']['url'])

app = Flask(__name__)

sms = SmsEngine(secrets=secrets)
w = Whatsapp(secrets=secrets, resources=resources)


@app.route('/')
def hello():
    print("Host-It Webhook")
    return "Host-It Webhook"


@app.route('/whatsapp_replies', methods=['POST'])
def receive_whatsapp():
    data = request.values
    # For now, we don't go too far. I just want to see if I received a message through there. I don't need to be able to answer.
    w.send_whatsapp_message(target_phone="+436601644192", body=f"""From {data["From"]}\n{data["Body"]}""")
    return "Forwarding successful!"


@app.route('/data_smoobu', methods=['POST'])
def receive_data_smoobu():
    """
    Get the call - classify it - call a dependent function.
    :return:
    """

    logging.info("\n\n\nNEW WEBHOOK CALL\n\n\n")
    data = request.json

    # Load the API:
    s = Smoobu(secrets=secrets,
               resources=resources)

    verify_signature()

    event_type = data["action"]
    unit_id = data["data"]["apartment"]["name"]
    guest_name = data["data"]["guest-name"].title()
    check_in = pd.Timestamp(data["data"]["arrival"])  # String
    check_out = pd.Timestamp(data["data"]["departure"])  # String
    adults = data["data"]["adults"]
    children = data["data"]["children"]
    n_nights = (check_out - check_in).days
    phone = data["data"]["phone"]

    logging.info(f"Event: {event_type}")
    logging.info(f"Unit ID: {unit_id}")
    logging.info(f"Guest: {guest_name}")
    logging.info(f"Check-In: {check_in}")
    logging.info(f"Check-Out: {check_out}")
    logging.info(f"adults: {adults}")
    logging.info(f"children: {children}")
    logging.info(f"Phone Number: {phone}")

    # Assign cleaner:
    cleaner_id = resources["apt_cleaners"][data["data"]["apartment"]["name"]]["name"]
    cleaner_phone = resources["apt_cleaners"][data["data"]["apartment"]["name"]]["phone_number"]
    logging.info(f"Cleaner Assigned: {cleaner_id}")

    if event_type == 'newReservation':
        w.message_owner(event="New Booking", unit_id=unit_id, name=guest_name, from_date=check_in, to_date=check_out, phone=phone)
        if within_n_days(n=14, date=check_in):
            bookings = s.get_smoobu_bookings(from_date=check_in, to_date=check_in, unit_id=unit_id, filter_by="check-out")
            if len(bookings):
                w.message_cleaner(event="change", unit_id=unit_id, job_date=check_in, cleaner_phone_number=cleaner_phone, next_guests_n_guests=adults + children, next_guests_n_nights=n_nights)
                update_cleanings_db(con=db_engine, action="change_guests", n_guests=adults + children, cleaner_id=cleaner_id, job_date=check_in, unit_id=unit_id)

        if within_n_days(n=14, date=check_out):
            bookings = s.get_smoobu_bookings(from_date=check_out, to_date=check_out, unit_id=unit_id, filter_by="check-in")
            if len(bookings):
                next_check_out = pd.Timestamp(bookings["departure"][0])
                next_check_in = pd.Timestamp(bookings["arrival"][0])
                next_nights = (next_check_out - next_check_in).days
                next_guests = int(bookings['adults'][0]) + int(bookings['children'][0])
                w.message_cleaner(event="new", unit_id=unit_id, job_date=check_out, cleaner_phone_number=cleaner_phone, next_guests_n_guests=next_guests, next_guests_n_nights=next_nights)
                update_cleanings_db(con=db_engine, action="add", n_guests=next_guests, cleaner_id=cleaner_id, job_date=check_out, unit_id=unit_id)
            else:
                max_guests = resources["apt_max_occupancy"][unit_id]
                max_nights = 3
                w.message_cleaner(event="new", unit_id=unit_id, job_date=check_out, cleaner_phone_number=cleaner_phone, next_guests_n_guests=max_guests, next_guests_n_nights=max_nights)
                update_cleanings_db(con=db_engine, action="add", n_guests=max_guests, cleaner_id=cleaner_id, job_date=check_out, unit_id=unit_id)

    elif event_type == 'cancelReservation':
        w.message_owner(event="Cancellation", unit_id=unit_id, name=guest_name, from_date=check_in, to_date=check_out, phone=phone)
        if within_n_days(n=14, date=check_in):
            bookings = s.get_smoobu_bookings(from_date=check_in, to_date=check_in, unit_id=unit_id, filter_by="check-out")
            if len(bookings):
                max_guests = resources["apt_max_occupancy"][unit_id]
                max_nights = 3
                w.message_cleaner(event="change", unit_id=unit_id, job_date=check_in, cleaner_phone_number=cleaner_phone, next_guests_n_guests=max_guests, next_guests_n_nights=max_nights)
                update_cleanings_db(con=db_engine, action="change_guests", n_guests=max_guests, cleaner_id=cleaner_id, job_date=check_out, unit_id=unit_id)

        if within_n_days(n=14, date=check_out):
            w.message_cleaner(event="cancel", unit_id=unit_id, job_date=check_out, cleaner_phone_number=cleaner_phone)
            update_cleanings_db(con=db_engine, action="cancel", cleaner_id=cleaner_id, job_date=check_out, unit_id=unit_id)

    return "Event processed successfully!"


def verify_signature():
    """
    ADD REQUEST SIGNATURE VERIFICATION!
    try:
        twil_sig = request.headers['X-Twilio-Signature']
        print(f"X-Twilio-Signature: {twil_sig}")
    except KeyError:
        return ('No X-Twilio-Signature. This request likely did not originate from Twilio.', 418)
    """
    pass


def within_n_days(n: int, date: pd.Timestamp):
    return (date <= (pd.Timestamp.now() + pd.Timedelta(days=n))) & (date >= pd.Timestamp.now())


def update_cleanings_db(con, action: str, **data):
    """
    :param con:
    :param action: One of "add", "change_guests", "change_date" and "cancel".
    :param data: Can contain cleaner_id, job_date, unit_id, n_guests.
    """
    if action == "add":
        cleaner_row = pd.DataFrame([{
            "personnel_id": data["cleaner_id"],
            "job_date": data["job_date"],
            "guests": data["n_guests"],
            "received_on": pd.Timestamp.now(),
            "unit_id": data["unit_id"],
            "job_type": "cleaning",
            "status": "booked",
            "paid": False
        }])
        cleaner_row.to_sql(name="cleanings",
                           con=con,
                           if_exists="append",
                           index=False)

    elif action == "change_guests":
        sql = f"""UPDATE cleanings SET guests = %(guests)s WHERE personnel_id = %(pid)s AND unit_id = %(unit)s AND job_date = %(job_date)s"""
        db_engine.execute(sql, {'guests': data["n_guests"], 'pid': data["cleaner_id"], 'unit': data["unit_id"],
                                'job_date': data["job_date"].strftime('%Y-%m-%d')})
        logging.info(f"Cleaning's guests amount updated.")

    elif action == "change_dates":
        sql = f"""UPDATE cleanings SET job_date = %(job_date_new)s WHERE personnel_id = %(pid)s AND unit_id = %(unit)s AND job_date = %(job_date)s"""
        db_engine.execute(sql, {'pid': data["cleaner_id"], 'unit': data["unit_id"],
                                'job_date_new': data["job_date"].strftime('%Y-%m-%d')})
        logging.info(f"Cleaning's date updated.")

    elif action == "cancel":
        sql = "UPDATE cleanings SET status = 'Canceled' WHERE personnel_id = %(pid)s AND unit_id = %(unit)s AND job_date = %(job_date)s"
        db_engine.execute(sql, {'pid': data["cleaner_id"], 'unit': data["unit_id"],
                                'job_date': data["job_date"].strftime('%Y-%m-%d')})
        logging.info(f"Status was changed to 'Canceled'.")

    else:
        ValueError("""Parameter action has to be one of "add", "change_guests", "change_date" or "cancel".""")


if __name__ == "__main__":
    app.run()
