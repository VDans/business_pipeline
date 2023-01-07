from flask import Flask, request, json
from flask_ngrok import run_with_ngrok
import logging
import datetime

from Messaging.twilio_sms import SmsEngine

logging.basicConfig(level=logging.INFO)

resources = json.load(open('Databases/resources_help.json'))

app = Flask(__name__)
# run_with_ngrok(app)  # Start ngrok when app is run?

sms = SmsEngine()


@app.route('/')
def hello():
    print("Hello World")


@app.route('/data', methods=['POST'])
def receive_data():
    """For now, get a notification and send an SMS with the infos"""
    data = request.json

    # Event
    try:
        event_name_display = resources["rentlio_events"][data["event"]["type"]]
    except KeyError:
        event_name_display = "Not found"
        logging.info(f"""event "{data["event"]["type"]}" unknown""")

    # Unit
    try:
        unit_id = resources["rentlio_properties"][str(data["event"]["payload"]["propertiesId"])]
    except KeyError:
        unit_id = "Not found"
        logging.info(f"""unit not found""")

    # Name
    try:
        guest_name = data["event"]["payload"]["guest"]["name"].title()
    except KeyError:
        guest_name = "Not found"
        logging.info(f"""Guest Name not found """)

    # Check-In
    try:
        check_in = data["event"]["payload"]["arrivalDate"]
        check_in = datetime.datetime.fromtimestamp(int(check_in))
    except KeyError:
        check_in = "Not found"
        logging.info(f"""check-in date not found """)

    # Check-Out
    try:
        check_out = data["event"]["payload"]["departureDate"]
        check_out = datetime.datetime.fromtimestamp(int(check_out))
    except KeyError:
        check_out = "Not found"
        logging.info(f"""check-out date not found """)

    sms.new_booking_sms(event=event_name_display,
                        unit=unit_id,
                        name=guest_name,
                        from_date=check_in.strftime("%Y-%m-%d"),
                        to_date=check_out.strftime("%Y-%m-%d"))

    return data


if __name__ == '__main__':
    app.run(debug=True)
