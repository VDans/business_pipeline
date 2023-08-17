import json
from google_api import Google

from message_scrapers.airbnb_message import AirbnbCom

secrets = json.load(open('config_secrets.json'))

a = AirbnbCom(secrets)
a.send_message(thread_id="1559808637", image_path="Resources/OMG10/OMG CHECKIN.png")
# g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])
