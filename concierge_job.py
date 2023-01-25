import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from Platforms.smoobu import Smoobu
from Messaging.twilio_whatsapp import Whatsapp

"""
The concierge should accompany guests along their trip.

Guests automatic communication: 
    1. Confirmation of booking: Platform
    2. Check-in Instructions: Platform
        --- Did the guest answer {n} days later?
            * YES: Preferred channel = Platform
            * NO: Send TEMPLATE message via Whatsapp.
            --- Is the template received (Error 69009 Twilio)?
                * YES: Preferred channel = Whatsapp
                * NO: Send check-in instructions via SMS
    3. FUTURE: {p} days before arrival: Send NUKI entry code.

Guests Help: 
    1. Through the platform: Answer manually with prepared template
    2. Through Whatsapp buttons: Help on Parking, additional services, etc. Common services!
    3. Through Whatsapp and SMS: Manual texting. If guests texts the bot, open a conversation with owner (Twilio conversation?)
    
Owner Control: 
    a) Owner should be able to deactivate all automations for a specific guest. Send whatsapp message "STOP {Guest ID}" to pause on that guest.
"""

try:
    secrets = json.load(open('/etc/config_secrets.json'))
    resources = json.load(open('business_pipeline/Databases/resources_help.json'))
except FileNotFoundError:
    secrets = json.load(open('config_secrets.json'))
    resources = json.load(open('Databases/resources_help.json'))

logging.basicConfig(level=logging.INFO)
db_engine = create_engine(url=secrets['database']['url'])

UNITS = ["EBS32", "HMG28", "GBS124", "GBS125"]


"""
If booking is soon, messages should come quicker..

Bot only starts when a person does NOT answer on the platform!
Therefore, record all guest messages, check the ones arriving soon, and filter for those who have NOT answered.

"""


def concierge():
    """
    This will be the function called every day.
    It checks if a guest arrives within 7 days.
    It checks the messages sent to that guest.
    """
    pass


def main():
    w = Whatsapp(secrets=secrets, resources=resources)
    smoo = Smoobu(secrets=secrets, resources=resources)


if __name__ == '__main__':
    main()