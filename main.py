import logging
import json
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from google_api import Google


logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))


message = Mail(
    from_email='office@host-it.at',
    to_emails='v.dans@outlook.be',
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
try:
    sg = SendGridAPIClient(api_key=secrets["twilio"]["email_api_key"])
    response = sg.send(message)
    print(response.status_code)
except Exception as e:
    print(e.message)
