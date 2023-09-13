import json
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
from google_api import Google

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def send_check_in_instructions(recipient_email: str, message: str):
    """For now, only for the properties on the system! The rest is handled by Smoobu."""
    message = Mail(
        from_email='office@host-it.at',
        to_emails=recipient_email,
        subject="Check-In instructions",
        html_content=message)
    try:
        sg = SendGridAPIClient(api_key=secrets["twilio"]["email_api_key"])
        response = sg.send(message)
        logging.info(f"Email sent to {recipient_email} with response: {response.status_code}")
    except Exception as e:
        logging.error(f"Email ERROR with response: {e}")


message = """<p>Hello!</p>
<p>I hope you are doing well.</p>
<p>Here are the instructions on how to check in. Let us know if you have any questions, we will be happy to help!<br></p>

<p>- CHECK-IN -</p>
<p>You can check in starting at 3pm and the apartment will be clean and ready for you.</p>
<p>Please come to Brabbeegasse 24, 1220 Vienna.</p>
<img src='https://github.com/VDans/business_pipeline/blob/master/Resources/BBG10/4aeedfc4-a516-4e46-b9a3-a9187800bad3.jpg?raw=true'>
<p>Check-in is via a lockbox.</p>
<p>From the parking lot, facing the building, take the walkway on the left side of the building.</p>
<p>At the end of the walkway, on the left side, by the green fence, you will find the lockbox with the password 4718. The key to all doors is in this lockbox.</p>
<p>Please take the key and the locker with you to the apartment. The apartment is located on the 1st floor number 10 (choose number 2 if you use the elevator).</p>

<p>Welcome!<br></p>

<p>- PARKING -</p>
<p>Please use the private parking spot 4.1. (Written on the floor).<br></p>

<p>- WIFI -</p>
<p>Login: Fritz!Box 7530 YR</p>
<p>PW: 64524713226047116712<br></p>

<p>- CHECK OUT -</p>
<p>Please check out before 11 am.</p>
<p>You can leave the keys in the apartment, thank you!<br></p>

<p>- HOUSE RULES -</p>
<p>-It is forbidden to smoke in the flat.</p>
<p>-No loud sounds or music in the flat after 20:00.</p>
<p>-No parties are allowed.</p>
<p>Any break in the above rules will result in a 200EUR fine on your mean of payment.<br></p>

<p>I hope you will have a wonderful time in the most livable city in the world.<br></p>

<p>With kind regards</p>
"""
send_check_in_instructions("valen@live.be", message=message)