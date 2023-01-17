import pandas as pd
import logging
from flask import Flask, request, json
from sqlalchemy import create_engine

from Messaging.twilio_sms import SmsEngine
from Messaging.twilio_whatsapp import Whatsapp
from Platforms.smoobu import Smoobu

logging.basicConfig(level=logging.INFO)

resources = json.load(open('Databases/resources_help.json'))
secrets = json.load(open('config_secrets.json'))

db_engine = create_engine(url=secrets['database']['url'])

app = Flask(__name__)

sms = SmsEngine(secrets=secrets)
w = Whatsapp(secrets=secrets, resources=resources)


@app.route('/')
def hello():
    print("Host-It Webhook")
    return "Host-It Webhook"


@app.route('/data_smoobu', methods=['POST'])
def receive_data_smoobu():
    """
    Get the call - classify it - call a dependent function.
    :return:
    """
    data = request.json

    # Load the API:
    s = Smoobu(secrets=secrets,
               resources=resources)

    verify_signature()

    event_type = data["data"]["type"]  # FixMe: This is not the correct key to use and leads only to modifications!
    unit_id = data["data"]["apartment"]["name"]
    guest_name = data["data"]["guest-name"].title()
    check_in = pd.Timestamp(data["data"]["arrival"])  # String
    check_out = pd.Timestamp(data["data"]["departure"])  # String
    adults = data["data"]["adults"]
    children = data["data"]["children"]

    logging.info(f"Event: {event_type}")
    logging.info(f"Unit ID: {unit_id}")
    logging.info(f"Guest: {guest_name}")
    logging.info(f"Check-In: {check_in}")
    logging.info(f"Check-Out: {check_out}")
    logging.info(f"adults: {adults}")
    logging.info(f"children: {children}")

    # Assign cleaner:
    cleaner_id = resources["apt_cleaners"][data["data"]["apartment"]["name"]]["name"]
    cleaner_phone = resources["apt_cleaners"][data["data"]["apartment"]["name"]]["phone_number"]
    logging.info(f"Cleaner Assigned: {cleaner_id}")

    if event_type == "reservation":
        if within_n_days(n=14, date=check_in):
            if is_event_on_day(api_connector=s, unit_id=unit_id, checked_event="check-out", date=check_in):
                w.text_cleaner(event="CHANGE", data={"checkin, checkout, adults, children"})
                text_owner(event="CHANGE", data={"checkin, checkout, adults, children"})
                update_cleanings_db("ADD or CHANGE")

        if within_n_days(n=14, date=check_out):
            if is_event_on_day(api_connector=s, unit_id=unit_id, checked_event="check-in", date=check_out):
                text_cleaner(event="NEW", data={"checkin, checkout, adults, children from request_booking"})
                text_owner(event="NEW", data={"checkin, checkout, adults, children from request_booking"})
                update_cleanings_db("ADD")
            else:
                text_cleaner(event="NEW", data={"checkin, checkout, adults, children from MAX capacity"})
                text_owner(event="NEW", data={"checkin, checkout, adults, children from MAX capacity"})
                update_cleanings_db("ADD")

    elif event_type == 'modification of booking':
        # Are you able to know what has changed? Add here. Otherwise, like new booking.
        pass

    elif event_type == "cancellation":
        if within_n_days(n=14, date=check_in):
            if is_event_on_day(api_connector=s, unit_id=unit_id, checked_event="check-out", date=check_in):
                text_cleaner(event="CHANGE", data={"checkin, checkout, adults, children to MAX occupancy"})
                text_owner(event="CANCEL + CHANGE", data={"checkin, checkout, adults, children to MAX occupancy"})
                update_cleanings_db("CHANGE to MAX occupancy")

        if within_n_days(n=14, date=check_out):
            text_cleaner(event="CANCEL", data={"canceled job date"}
            text_owner(event="CANCEL", data={"canceled job date"}
            update_cleanings_db("CHANGE to canceled")


            if event_type in ["reservation", "modification of booking"]:

            ##############################################
            # Finding the previous booking on the same day
            # Only IF check-in on the same day as previous check-out (which, by default, was set on max capacity)
                past_booking = s.get_smoobu_bookings(from_date=check_in,
                                                     to_date=check_in,
                                                     unit_id=unit_id,
                                                     filter_by="check-out")

            if len(past_booking) == 0:
                logging.info(f"No one is checking out on the day of arrival of the guest. Nothing has to be changed.")

            else:
                logging.info(f"Someone is checking out on the day of arrival of the guest. The cleaning has to be updated.")
            # If the booking arrival is within 14 days, warn the staff.
            if (check_in <= (pd.Timestamp.now() + pd.Timedelta(days=14))) & (check_in >= pd.Timestamp.now()):
                sms.cleaner_sms(event=event_type,
                                unit_id=unit_id,
                                job_date=check_in,
                                next_guests_n_nights=(check_out - check_in).days,
                                next_guests_n_guests=data["data"]['adults'] + data["data"]['children'],
                                cleaner_phone_number=cleaner_phone)
            logging.info(f"Cleaner has been texted successfully")

            # Text owner to confirm.
            sms.new_booking_sms(event=event_type,
                                unit=unit_id,
                                name=guest_name,
                                from_date=check_in,
                                to_date=check_out,
                                phone=data["data"]["phone"])

            # The relevant cleaning therefore has to be changed.
            logging.info(f"The cleaning right before check-in has been modified")

            ##############################################
            # Finding the NEXT booking, if on the same day
            next_booking = s.get_smoobu_bookings(from_date=check_out,
                                                 to_date=check_out,
                                                 unit_id=unit_id,
                                                 filter_by="check-in")

            if len(next_booking) == 0:
            # In this case use the max occupancy:
                logging.info(f"No one is arriving on that check-out day. Assigning max capacity: {resources['apt_max_occupancy'][unit_id]} guests")
            next_guests = resources["apt_max_occupancy"][unit_id]
            next_nights = 3  # Default value

            else:
            # Record the info of the next guests
            next_check_out = pd.Timestamp(next_booking["departure"][0])
            next_check_in = pd.Timestamp(next_booking["arrival"][0])
            next_nights = (next_check_out - next_check_in).days
            next_guests = int(next_booking['adults'][0]) + int(next_booking['children'][0])
            logging.info(f"The next booking has {next_guests} guests and stay {next_nights} nights")

            # If the booking arrival is within 14 days, warn the staff.
            if (check_out <= (pd.Timestamp.now() + pd.Timedelta(days=14))) & (check_out >= pd.Timestamp.now()):
                sms.cleaner_sms(event=event_type,
                                unit_id=unit_id,
                                job_date=check_out,
                                next_guests_n_nights=next_nights,
                                next_guests_n_guests=next_guests,
                                cleaner_phone_number=cleaner_phone)
            logging.info(f"Cleaner has been texted successfully")

            # Text owner to confirm.
            sms.new_booking_sms(event=event_type,
                                unit=unit_id,
                                name=guest_name,
                                from_date=check_in,
                                to_date=check_out,
                                phone=data["data"]["phone"])

            # Cleaner has been informed, therefore add the job to the relevant db table:
            cleaner_row = pd.DataFrame([{
                "personnel_id": cleaner_id,
                "job_date": check_out,
                "received_on": pd.Timestamp.now(),
                "unit_id": unit_id,
                "job_type": "cleaning",
                "status": "booked",
                "paid": False
            }])
            cleaner_row.to_sql(name="cleanings",
                               con=db_engine,
                               if_exists="append",
                               index=False)

            elif event_type == "cancellation":
            cleaner_id = resources["apt_cleaners"][data["data"]["apartment"]["name"]]["name"]
            cleaner_phone = resources["apt_cleaners"][data["data"]["apartment"]["name"]]["phone_number"]

            # Modify the previous cleaning to maximum occupancy:
            past_booking = s.get_smoobu_bookings(from_date=check_in,
                                                 to_date=check_in,
                                                 unit_id=unit_id,
                                                 filter_by="check-out")

            if len(past_booking) == 0:
                logging.info(f"No one is checking out on the day of arrival of the guest. Nothing has to be changed.")

            else:
                logging.info(f"Someone is checking out on the day of arrival of the guest. The cleaning has to be updated to max occupancy.")
            # If the booking arrival is within 14 days, warn the staff.
            if (check_in <= (pd.Timestamp.now() + pd.Timedelta(days=14))) & (check_in >= pd.Timestamp.now()):
                sms.cleaner_sms(event="modification of booking",
                                unit_id=unit_id,
                                job_date=check_in,
                                next_guests_n_nights=3,
                                next_guests_n_guests=resources["apt_max_occupancy"][unit_id],
                                cleaner_phone_number=cleaner_phone)
            logging.info(f"Cleaner has been texted successfully")

            # Text owner to confirm.
            sms.new_booking_sms(event="modification of booking",
                                unit=unit_id,
                                name=guest_name,
                                from_date=check_in,
                                to_date=check_out,
                                phone=data["data"]["phone"])

            # The relevant cleaning therefore has to be changed.
            logging.info(f"The cleaning right before check-in has been modified")

            # If the booking arrival is within 14 days, warn the staff of the cancellation
            if (check_out <= (pd.Timestamp.now() + pd.Timedelta(days=14))) & (check_out >= pd.Timestamp.now()):
                sms.cleaner_sms(event=event_type,
                                unit_id=unit_id,
                                job_date=check_out,
                                cleaner_phone_number=cleaner_phone)
            logging.info(f"Cleaner has been texted successfully")

            # Text owner to confirm.
            sms.new_booking_sms(event=event_type,
                                unit=unit_id,
                                name=guest_name,
                                from_date=check_in,
                                to_date=check_out,
                                phone=data["data"]["phone"])

            # Cleaner has been informed, therefore change the job in the relevant db table:
            sql = "UPDATE cleanings SET status = 'Canceled' WHERE personnel_id = %(pid)s AND unit_id = %(unit)s AND job_date = %(job_date)s"
            db_engine.execute(sql, {'pid': cleaner_id, 'unit': unit_id, 'job_date': check_out})
            logging.info(f"{guest_name}'s status was changed to 'Canceled'.")

            else:
            ValueError("Call Type unknown")

    return data


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

def is_event_on_day(api_connector, unit_id, date: pd.Timestamp, checked_event: str = "check-out"):
    f"""
    Is there a {checked_event} on the {date}?
    """
    out = api_connector.get_smoobu_bookings(from_date=date,
                                            to_date=date,
                                            unit_id=unit_id,
                                            filter_by=checked_event)
    return len(out) > 0

if __name__ == "__main__":
    app.run()
