import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

logging.basicConfig(level=logging.INFO)

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)
z = Zodomus(secrets=secrets)

# response = z.get_channels()
# response = z.custom_api_call("POST", "/property-cancellation", json.dumps({"channelId": "1", "propertyId": "999"}))
# response = z.activate_property(channel_id="1", unit_id_z="10204823")  # Token for Airbnb?
# response = z.check_property(channel_id="1", unit_id_z="10204823")
# response = z.activate_room(channel_id="1", unit_id_z="10204823", room_id_z="99902")
# response = z.check_property(channel_id="1", unit_id_z="10204823")  # Checks if the mapping was done correctly.
# response = z.get_rooms_rates(channel_id="1", unit_id_z="10204823")  # Checks if the mapping was done correctly.
# response = z.set_availability(unit_id_z="10204823", date_from=pd.Timestamp(day=1, month=7, year=2023), date_to=pd.Timestamp(day=5, month=7, year=2023), availability=0)
# response = z.set_rate(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][FLAT]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][FLAT]["roomId"], rate_id_z=secrets["booking"]["flat_ids"][FLAT]["rateId"], date_from=pd.Timestamp(day=30, month=6, year=2023), date_to=pd.Timestamp(day=1, month=7, year=2023), price=90)
# response = z.get_reservations_summary(channel_id="1", unit_id_z="10204823")
# response = z.get_reservations_queue(channel_id="1", unit_id_z="10204823")
response = z.get_reservation(channel_id="1", unit_id_z="999", reservation_number="999001").json()
dbh.upload_reservation(channel_id_z="1", unit_id_z="999", reservation_z=response)
