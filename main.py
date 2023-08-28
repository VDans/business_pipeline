import pandas as pd
import json
import logging

from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
from google_api import Google


def add_write_snippet(booking_date, google, data, flat, value):
    cell_range = google.get_rolling_range(unit_id=flat, date1=booking_date, col=secrets["flats"][flat]["pricing_col"], headers_rows=3)
    snippet = {
        "range": cell_range,
        "values": [
            [value]
        ]
    }
    data.append(snippet)


def get_n_guests(reservation_z):
    """Get number of guests from shitty reservation format"""
    data = reservation_z["reservations"]
    # The way n_adults and n_children are written is shameful in the API...
    adults = 0
    children = 0
    guests = data["rooms"][0]["guestCount"]  # List of dicts
    for g in guests:
        if g["adult"] == 1:
            adults += int(g["count"])
        else:
            children += int(g["count"])
    return adults + children


secrets = json.load(open('config_secrets.json'))

flat_name = 'LFG25'
channel_name = 'Booking'
data = {
    "channelId": "1",
    "propertyId": "10633058",
    "reservationId": "1735434542"
}

z = Zodomus(secrets=secrets)
g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)
offset = g.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15)) - 3  # Rolling window. -3 for 3 headers rows!
# Find the reservation
reservation_z = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"],
                                  reservation_number=data["reservationId"]).json()
logging.info("Retrieved reservation data")

# Initiate bookings table
tbl = Table("bookings", MetaData(), autoload_with=db_engine)

# logging.info(f"Step 1 finishes at timestamp {time.time() - start_time} seconds.")

# Exception at GBS... If other exceptions appear, change pid/rid logic.
if (flat_name == "GBS") and (channel_name == "Booking"):
    flat_name = [fn for fn in secrets['flats'] if
                 secrets["flats"][fn]["rid_booking"] == reservation_z["reservations"]["rooms"][0]["id"]][0]

logging.info(
    f"New booking in {flat_name} from {reservation_z['reservations']['rooms'][0]['arrivalDate']} to {reservation_z['reservations']['rooms'][0]['departureDate']}")

# Upload the reservation data to the DB:
try:
    dbh.upload_reservation(channel_id_z=data["channelId"], flat_name=flat_name, reservation_z=reservation_z)
    logging.info("Reservation data uploaded to table -bookings-")
except Exception as e:
    logging.error(f"Could NOT upload the new reservation to DB: {e}")

# Extract dates from data:
date_from = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["arrivalDate"])
date_to = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["departureDate"])

# Close the dates on both platforms:
z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"],
                   room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=date_from, date_to=date_to,
                   availability=0)
z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"],
                   room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=date_from,
                   date_to=date_to + pd.Timedelta(days=-1), availability=0)
logging.info("Availability has been closed in both channels")

# logging.info(f"Step 2 finishes at timestamp {time.time() - start_time} seconds.")

# In the Google Pricing Sheet:
# Write the name of the guest:
try:
    part2 = " " + reservation_z["reservations"]["customer"]["lastName"].title()[0] + "."
except IndexError as ie:
    logging.error(f"ERROR: {ie}")
    part2 = ""
short_name = f"""{reservation_z["reservations"]["customer"]["firstName"].title()}{part2} ({channel_name[0]})"""
try:
    dates_range = pd.Series(pd.date_range(start=date_from, end=(date_to - pd.Timedelta(days=1))))
    dat = []
    dates_range.apply(add_write_snippet, args=(g, dat, flat_name, short_name))
    g.batch_write_to_cell(data=dat)

except Exception as ex:
    logging.warning(f"Could not write to sheet: {ex}")
# logging.info(f"Step 3 finishes at timestamp {time.time() - start_time} seconds.")

# Get n_guests
try:
    n_guests = get_n_guests(reservation_z)
    logging.info(f"There are {n_guests} guests.")
except Exception as e:
    logging.warning(f"Couldn't obtain number of guests: {e}")
    n_guests = -1

# Merge the cells based on the first one:
try:
    g.merge_cells2(date_from, date_to, flat_name, offset)
except Exception as ex:
    logging.error(f"Could not merge cells with exception: {ex}")
# logging.info(f"Step 4 finishes at timestamp {time.time() - start_time} seconds.")

try:
    cleaning_fee = dbh.extract_cleaning_fee(channel_id_z=str(data["channelId"]), reservation_z=reservation_z,
                                            flat_name=flat_name)
    total_price = float(reservation_z["reservations"]["rooms"][0]["totalPrice"]) + cleaning_fee
    g.write_note2(date_from, date_from, flat_name,
                  f"""{reservation_z["reservations"]["customer"]["firstName"].title()} {reservation_z["reservations"]["customer"]["lastName"].title()}\nPaid {total_price}€\nGuests: {n_guests}\nID: {data["reservationId"]}""",
                  offset=offset)
except Exception as ex:
    logging.error(f"Could not write note! Exception: {ex}")

logging.info(f"Wrote '{channel_name}' within the pricing Google Sheet. Added info note.")
# logging.info(f"Step 5 finishes at timestamp {time.time() - start_time} seconds.")


