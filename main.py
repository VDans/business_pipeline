import json
import logging
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)

data = {
    "reservationId": "2021620444",
    "channelId": "1"
}

tbl = Table("bookings", MetaData(), autoload_with=db_engine)

z = Zodomus(secrets=secrets)
# reservation_z = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"]).json()

with db_engine.begin() as conn:
    try:
        # Fetch the updated reservation price
        upd3 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(nights_price=float(reservation_z['reservations']['reservation']['totalPrice'].replace(",", "")))
        # Update the platform commission
        r_com = -0.15 if str(data["channelId"]) == '3' else -0.162
        upd4 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(commission_host=float(reservation_z['reservations']['reservation']['totalPrice'].replace(",", "")) * r_com)
    except Exception as ex:
        upd3 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(nights_price=0)
        upd4 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(commission_host=0)
        logging.error(f"Couldn't update prices: {ex}")
    finally:
        conn.execute(upd3)
        logging.info(f"UPDATE bookings SET nights_price = {reservation_z['reservations']['reservation']['totalPrice']} WHERE booking_id = '{data['reservationId']}'")
        conn.execute(upd4)
        logging.info(f"UPDATE bookings SET commission_host = 15% nets WHERE booking_id = '{data['reservationId']}'")
