import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

from google_api import Google

logging.basicConfig(level=logging.INFO)

secrets = json.load(open('config_secrets.json'))
# db_engine = create_engine(url=secrets["database"]["url"])
# dbh = DatabaseHandler(db_engine, secrets)
# z = Zodomus(secrets=secrets)
g = Google(secrets)
# response = g.merge_cells(3, 5, 7, 8, internal_sheet_id=920578163)
response = g.unmerge_cells(3, 5, 7, 8, internal_sheet_id=920578163)
# g.write_to_cell(cell_range="E39", value=4)

# dates = list(pd.date_range(start=pd.Timestamp(day=1, month=8, year=2023), end=pd.Timestamp(day=3, month=8, year=2023)))
# for d in dates:
# 	cell_range = g.get_pricing_range(unit_id="LORY22",
# 	                                 date1=d)
# 	response = g.write_to_cell(cell_range)

# response = z.get_channels()
# response = z.custom_api_call("POST", "/property-cancellation", json.dumps({"channelId": "1", "propertyId": "999"}))
# response = z.activate_property(channel_id="3", unit_id_z="10204823")  # Token for Airbnb?
# response = z.check_property(channel_id="3", unit_id_z="923146811873597880")
# response = z.activate_room(channel_id="1", unit_id_z="10204823", room_id_z="99902")
# response = z.check_property(channel_id="3", unit_id_z="923146811873597880")  # Checks if the mapping was done correctly.
# response = z.get_rooms_rates(channel_id="1", unit_id_z="10204823")  # Checks if the mapping was done correctly.
# response = z.set_availability(channel_id="3", unit_id_z="25053383", room_id_z="2505338301", date_from=pd.Timestamp(day=30, month=6, year=2023), date_to=pd.Timestamp(day=1, month=7, year=2023), availability=1)
# response = z.set_rate(channel_id="1", unit_id_z=secrets["booking"]["flat_ids"][FLAT]["propertyId"], room_id_z=secrets["booking"]["flat_ids"][FLAT]["roomId"], rate_id_z=secrets["booking"]["flat_ids"][FLAT]["rateId"], date_from=pd.Timestamp(day=30, month=6, year=2023), date_to=pd.Timestamp(day=1, month=7, year=2023), price=90)
# response = z.get_reservations_summary(channel_id="3", unit_id_z="923146811873597880")
# response = z.get_reservations_queue(channel_id="1", unit_id_z="10204823")
# response = z.get_reservation(channel_id="1", unit_id_z="999", reservation_number="999001").json()
# dbh.upload_reservation(channel_id_z="1", unit_id_z="999", reservation_z=response)

print(response)
# print(json.dumps(response.json(), indent=3))
