from flask import Flask, request, json
from flask_ngrok import run_with_ngrok
import logging
import datetime

from Messaging.twilio_sms import SmsEngine

logging.basicConfig(level=logging.INFO)

resources = json.load(open('Databases/resources_help.json'))
secrets = json.load(open('config_secrets.json'))

app = Flask(__name__)

sms = SmsEngine(secrets=secrets)


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
    data = request.json["data"]

    call_type = data["type"]


    print("")

    # Event
    try:
        event_name_display = resources["smoobu_events"][data["type"]]
    except KeyError:
        event_name_display = "Not found"
        logging.info(f"""event "{data["type"]}" unknown""")

    # Unit
    try:
        unit_id = str(data["apartment"]["name"])
    except KeyError:
        unit_id = "Not found"
        logging.info(f"""unit not found""")

    # Name
    try:
        guest_name = data["guest-name"].title()
    except KeyError:
        guest_name = "Not found"
        logging.info(f"""Guest Name not found """)

    # Check-In
    try:
        check_in = data["arrival"]  # String already
    except KeyError:
        check_in = "Not found"
        logging.info(f"""check-in date not found """)

    # Check-Out
    try:
        check_out = data["departure"]
    except KeyError:
        check_out = "Not found"
        logging.info(f"""check-out date not found """)

    sms.new_booking_sms(event=event_name_display,
                        unit=unit_id,
                        name=guest_name,
                        from_date=check_in,
                        to_date=check_out)

    return data


if __name__ == "__main__":
    app.run()