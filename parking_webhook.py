import time

from flask import Flask, request, json
from datetime import datetime
# from sqlalchemy import create_engine

from Messaging.twilio_sms import SmsEngine
from Messaging.twilio_whatsapp import Whatsapp

try:
    secrets = json.load(open('/etc/config_secrets.json'))
    resources = json.load(open('Databases/resources_help.json'))
except FileNotFoundError:
    secrets = json.load(open('config_secrets.json'))
    resources = json.load(open('Databases/resources_help.json'))

# db_engine = create_engine(url=secrets['database']['url'])

app = Flask(__name__)

sms = SmsEngine(secrets=secrets, resources=resources)
w = Whatsapp(secrets=secrets, resources=resources)


@app.route('/')
def hello():
    return "Parking Webhook"


@app.route('/whatsapp_parking', methods=['POST'])
def whatsapp_parking():
    """
    Receive instructions from user.
    """
    data = request.values

    if "help" in data["Body"]:
        w.send_whatsapp_message(target_phone="+436601644192",
                                body="""Format: {plate_number without space} // {yyyy-mm-dd hh-mm}""")
    else:
        # 1. Capture parameters from user instructions:
        plate = data["Body"].split(sep="//")[0].strip()
        expiration_timestamp = data["Body"].split(sep="//")[1].strip()
        expiration_timestamp = datetime.strptime(expiration_timestamp, '%Y-%m-%d %H:%M')

        # 2. Book the parking through text message if the expiration timestamp has not been reached yet:
        # You only want to send a text every 17 minutes! Limit the costs.
        if datetime.now() <= expiration_timestamp:
            sms.send_parking_booking(plate=plate)

            while True:
                time.sleep(17*60)
                sms.send_parking_booking(plate=plate)
                if datetime.now() > expiration_timestamp:
                    break

        else:
            w.send_whatsapp_message(target_phone="+436601644192",
                                    body=f"The time you instructed is in the past! It is {datetime.now().strftime('%H:%M')} right now.")

        w.send_whatsapp_message(target_phone="+436601644192",
                                body=f"Your expiration time has arrived. Thank you for using Parking Bot")

    return "Forwarding successful!"


@app.route('/sms_redirect', methods=['POST'])
def sms_redirect():
    """
    Receive SMS on Twilio and redirect to personal Whatsapp.
    """
    data = request.values
    w.send_whatsapp_message(target_phone="+436601644192", body=f"""New SMS from {data["From"]}:\n{data["Body"]}""")
    return "Forwarding successful!"


if __name__ == "__main__":
    app.run()
