from flask import Flask, request, json
from flask_ngrok import run_with_ngrok
from manager import Manager
from Messaging.twilio_sms import SmsEngine

app = Flask(__name__)
run_with_ngrok(app)  # Start ngrok when app is run?

sms = SmsEngine()

@app.route('/')
def hello():
    return "I    see    you."


@app.route('/receive_data', methods=['POST'])
def receive_data():
    """For now, get a notification and send an SMS with the infos"""
    data = request.json
    sms.new_booking_sms(unit="EBS32",
                        name="Sara Gratt",
                        from_date="2022-10-01",
                        to_date="2023-01-01")

    return data


if __name__ == '__main__':
    app.run(debug=True)
