import logging
import json
import pandas as pd
from zodomus_api import Zodomus

secrets = json.load(open('config_secrets.json'))

logging.basicConfig(level=logging.INFO)

z = Zodomus(secrets=secrets)
# response = z.get_channels()
# response = z.custom_api_call("POST", "/property-cancellation", json.dumps({"channelId": "1", "propertyId": "999"}))
# response = z.activate_property(channel_id="1", unit_id_z="10204823")  # Token for Airbnb?
# response = z.check_property(channel_id="1", unit_id_z="10204823")
# response = z.activate_room(channel_id="1", unit_id_z="10204823", room_id_z="99902")
# response = z.check_property(channel_id="1", unit_id_z="10204823")  # Checks if the mapping was done correctly.
# response = z.get_rooms_rates(channel_id="1", unit_id_z="10204823")  # Checks if the mapping was done correctly.
# response = z.set_availability(unit_id_z="10204823", date_from=pd.Timestamp(day=1, month=7, year=2023), date_to=pd.Timestamp(day=5, month=7, year=2023), availability=0)
response = z.set_rate(channel_id="1", unit_id_z="999", room_id_z="1020482301", date_from=pd.Timestamp(day=15, month=6, year=2023), date_to=pd.Timestamp(day=16, month=6, year=2023), price=500)
# response = z.get_reservations_summary(channel_id="1", unit_id_z="10204823")
# response = z.get_reservations_queue(channel_id="1", unit_id_z="10204823")
# response = z.get_reservation(channel_id="1", unit_id_z="10204823", reservation_number="2139286439")

print(json.dumps(response.json(), indent=3))
