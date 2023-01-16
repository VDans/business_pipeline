"""
This job should run every morning, and alert the cleaning staff of any new cleanings within the next 2 weeks.
"""
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from Platforms.smoobu import Smoobu
from Messaging.twilio_sms import SmsEngine

secrets = json.load(open('/etc/config_secrets.json'))
resources = json.load(open('Databases/resources_help.json'))

logging.basicConfig(level=logging.INFO)
db_engine = create_engine(url=secrets['database']['url'])

UNITS = ["EBS32", "HMG28", "GBS124", "GBS125"]


def check_new_cleanings(unit_id, sms_engine, api_connector):
    """
    For each apartment - cleaner:

    Get the check-out dates in the next two weeks.
    Get the planned cleanings in the next two weeks.
    """
    # Check-Outs
    bookings = api_connector.get_smoobu_bookings(from_date=pd.Timestamp.now(),
                                                 to_date=pd.Timestamp.now() + pd.Timedelta(days=14),
                                                 unit_id=unit_id)
    checkouts = pd.to_datetime(bookings["departure"])

    # Cleanings
    cleanings = pd.read_sql(sql=f"""SELECT * FROM cleanings WHERE unit_id = {unit_id}""",
                            con=db_engine,
                            index_col=None)
    cleanings = pd.to_datetime(cleanings["job_date"])

    missing_cleanings = [c for c in checkouts if c not in cleanings]

    if len(missing_cleanings) == 0:
        for m in missing_cleanings:
            # Each one missing should be added to the DB + texted to the relevant staff.
            # 1. Find the checkout cleaning info:
            new_cleaning = bookings[bookings["departure"] == m.strftime("%Y-%m-%d")]

            # Check if someone checks-in after the new guest. If yes, text this data. If not, text the max occupancy.
            next_booking = api_connector.get_smoobu_bookings(from_date=new_cleaning["departure"],
                                                             to_date=new_cleaning["departure"],
                                                             unit_id=unit_id,
                                                             filter_by="check-in")
            if len(next_booking) == 0:
                # In this case use the max occupancy:
                logging.info(f"No one is arriving on that check-out day. Assigning max capacity: {resources['apt_max_occupancy'][unit_id]}")
                next_guests = resources["apt_max_occupancy"][unit_id]
                next_nights = 3  # Default value
            else:
                # Record the info of the next guests
                next_check_out = pd.Timestamp(next_booking["departure"][0])
                next_check_in = pd.Timestamp(next_booking["arrival"][0])
                next_nights = (next_check_out - next_check_in).days
                next_guests = int(next_booking['adults'][0]) + int(next_booking['children'][0])
                logging.info(f"The next booking has {next_guests} guests and stay {next_nights} nights")

            cleaner_phone: str = resources["apt_cleaners"][unit_id]["phone_number"]
            sms_engine.cleaner_sms(event="reservation",
                                   unit_id=unit_id,
                                   job_date=m,
                                   cleaner_phone_number=cleaner_phone,
                                   next_guests_n_nights=next_nights,
                                   next_guests_n_guests=next_guests)
    else:
        logging.info(f"No new cleaning detected for {unit_id} within 14 days.")


def main():
    sms = SmsEngine(secrets=secrets)
    smoo = Smoobu(secrets=secrets, resources=resources)

    for u in UNITS:
        check_new_cleanings(unit_id=u, sms_engine=sms, api_connector=smoo)


if __name__ == '__main__':
    main()
