import logging
import json
import pandas as pd
from zodomus_api import Zodomus


logging.basicConfig(level=logging.INFO)

z = Zodomus()
response = z.get_channels()
response = z.activate_property(channel_id="1", unit_id_z="999")  # Token for Airbnb?
response = z.check_property(channel_id="1", unit_id_z="999")
response = z.activate_room(channel_id="1", unit_id_z="999", room_id_z="99901")
response = z.check_property(channel_id="1", unit_id_z="999")  # Checks if the mapping was done correctly.
response = z.get_rooms_rates(channel_id="1", unit_id_z="999")  # Checks if the mapping was done correctly.
response = z.set_availability(unit_id_z="999", date_from=pd.Timestamp(day=15, month=6, year=2023), date_to=pd.Timestamp(day=16, month=6, year=2023), availability=0)
response = z.set_rate(channel_id="1", unit_id_z="999", room_id_z="99901", date_from=pd.Timestamp(day=15, month=6, year=2023), date_to=pd.Timestamp(day=16, month=6, year=2023), price=500)
response = z.get_reservations_summary(channel_id="1", unit_id_z="999")
response = z.get_reservations_queue(channel_id="1", unit_id_z="999")
response = z.get_reservation(channel_id="1", unit_id_z="999", reservation_number="9991777")

print(json.dumps(response.json(), indent=3))
