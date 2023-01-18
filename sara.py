"""
This job should run every morning, and alert the cleaning staff of any new cleanings within the next 2 weeks.
"""
import json
import logging
import random
import requests
from sqlalchemy import create_engine
from Messaging.twilio_whatsapp import Whatsapp

try:
    secrets = json.load(open('/etc/config_secrets.json'))
    resources = json.load(open('business_pipeline/Databases/resources_help.json'))
except FileNotFoundError:
    secrets = json.load(open('config_secrets.json'))
    resources = json.load(open('Databases/resources_help.json'))

logging.basicConfig(level=logging.INFO)
db_engine = create_engine(url=secrets['database']['url'])
url = "https://dog.ceo/api/breeds/image/random"


def send_random_love_message(whatsapp_engine):
    """
    Step 1: Pull a random love picture
    Step 2: Determine a random time between 10AM and 15AM
    Step 3: Send the whatsapp message
    """

    # Message
    response = requests.request(method="GET", url=url)
    image_url = response.json()["message"]
    n_repeat = random.randint(0, 4)
    ejis = ["❤️", "🥰", "🧸❤️", "💖", "👩‍❤️‍👨"]
    hearts_drawing = random.randint(1, 5)
    body = n_repeat * ejis[hearts_drawing]

    whatsapp_engine.send_whatsapp_message(target_phone="+436643964372", body=body, media_url=image_url)


def main():
    w = Whatsapp(secrets=secrets, resources=resources)
    send_random_love_message(whatsapp_engine=w)


if __name__ == '__main__':
    main()
