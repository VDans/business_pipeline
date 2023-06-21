import logging
import json
from Messaging.twilio_whatsapp import Whatsapp
from twilio.rest import Client

logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))


client = Client(secrets['twilio']['account_sid'], secrets['twilio']['auth_token'])
client.messages.create(from_="whatsapp:+436703085269",
                       to=f"whatsapp:+436601644192",
                       body="HELLO!")
