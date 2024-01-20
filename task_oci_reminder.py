import json
import logging
import pandas as pd
from twilio.rest import Client
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None


def send_sms(booking, c):
    try:
        logging.info(f"Sending SMS to {booking['phone']}")
        if booking["phone"][:3] in ["+43", "+49"]:
            message_body = \
f"""Hallo!
Ich bin der Gastgeber für Ihr Apartment in Wien.
Ich habe bemerkt, dass Sie meine Nachrichten auf {booking["platform"]} nicht gesehen haben.
Daher sende ich Ihnen hier eine Erinnerung, dies Online-Check-in-Formular auszufüllen.

Https://hostit.typeform.com/online-checkin

Ihre Buchungsnummer lautet: {booking["booking_id"]}
Sobald dies geschehen ist, werden Sie auf {booking["platform"]} genaue Anweisungen erhalten, wie Sie Ihre Wohnung betreten können.
Einen schönen Tag noch!"""
        else:
            message_body = \
f"""Hello!
This is the host for your apartment in Vienna.
We noticed that you have not seen our messages on {booking["platform"]}.
Therefore, we send you here a reminder to fill our online check-in form:

Https://hostit.typeform.com/online-checkin

Your booking number is: {booking["booking_id"]}
Once this is done, you will receive precise instructions on {booking["platform"]} on how to enter your flat.
Have a good day!"""

        response = c.messages.create(
            from_="+436703085269",
            to=booking["phone"],
            body=message_body
        )
    except Exception as ex:
        response = ex
        logging.error(f"ERROR: {ex}")

    return response


def send_sms_manager(booking, s):
    logging.info(f"Sending email to office@host-it.at")
    guests = ""
    for i in range(len(booking["phone"])):
        guests += f"<p>{booking['object'][i]}: {booking['guest_name'][i]} - {booking['reservation_start'][i].strftime('%Y-%m-%d')} - {booking['booking_id'][i]}</p>"

    message_body = \
        f"""{len(booking["phone"])} guest/s have not filled the online check-in: {guests}"""

    message = Mail(
        from_email='office@host-it.at',
        to_emails=["office@host-it.at"],
        subject="Daily Missing Online Check-Ins",
        html_content=message_body)

    sg = SendGridAPIClient(api_key=s["twilio"]["email_api_key"])
    response = sg.send(message)
    return response


def remind_guest():
    """
    A/
    This task runs once a day at 11:00. It:
    1) Identify guests checking in exactly 3 days.
    2) Send Whatsapp + SMS to provided number, giving them their booking_id.
    """
    secrets = json.load(open('config_secrets.json'))
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)
    account_sid = secrets["twilio"]["account_sid"]
    auth_token = secrets["twilio"]["auth_token"]

    client = Client(account_sid, auth_token)

    # Get bookings in exactly 3 days
    sql = open("sql/task_oci_reminder.sql").read()
    bookings_3days = dbh.query_data(sql=sql, dtypes={"reservation_start": pd.Timestamp})
    logging.info(f"{len(bookings_3days['phone'])} guest/s have not filled the online check-in and arrive in 3 days.")

    # For each booking (list of timestamps), send a POST request to close the dates:
    bookings_3days.apply(send_sms, axis=1, args=(client,))


def remind_manager():
    """
    B/
    1) Identify guests checking in < 3 days.
    2) Send Whatsapp to Val, with name, booking_id.
    """
    secrets = json.load(open('config_secrets.json'))
    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)
    account_sid = secrets["twilio"]["account_sid"]
    auth_token = secrets["twilio"]["auth_token"]

    client = Client(account_sid, auth_token)

    # Get bookings in exactly 3 days
    sql = open("sql/task_oci_reminder2.sql").read()
    bookings_2days = dbh.query_data(sql=sql, dtypes={"reservation_start": pd.Timestamp})
    logging.info(f"{len(bookings_2days['phone'])} guest/s have not filled the online check-in and arrive in < 3 days.")

    # For each booking (list of timestamps), send a POST request to close the dates:
    send_sms_manager(bookings_2days, secrets)


remind_guest()
remind_manager()
