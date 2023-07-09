import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

from google_api import Google

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def write_cleanings():
    """
    This task runs once a day.

    Get flat name, checkout date, and number of guests from each reservation.
    For each, find out the correct Google workbook id and write to the correct cell according to the flat and date.
    """
    sql = open("sql/task_cleanings.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_end": pd.Timestamp})

    flats = list(bookings["object"].unique())
    for flat in flats:
        logging.info(f"Processing cleanings in flat {flat}")

        # Find the corresponding Google sheet for the specific cleaner:
        g = Google(secrets=secrets, workbook_id=secrets["flats"][flat]["cleaning_workbook_id"])

        # Remove all current values (avoid confusion):
        col_range = f"{secrets['flats'][flat]['cleaning_col']}2:{secrets['flats'][flat]['cleaning_col']}900"
        response = g.clear_range(cell_range=col_range)
        logging.info(f"Removed all current values")

        # Go over the available flats
        b = bookings[bookings["object"] == flat]

        # Shift the n_guests for each flat:
        b['n_guests'] = b['n_guests'].shift(-1, fill_value=-1)

        # Write the new values:
        b.apply(write_cleaning_schedule, axis=1, args=(flat, g))

    logging.info("Processed all cleanings within 31 days.")


def write_cleaning_schedule(booking, flat, google):
    logging.info(f"-- Adding {booking['reservation_end'].strftime('%Y-%m-%d')}")

    # Write to the correct date the number of guests:
    cell_range = google.get_pricing_range(unit_id=flat, date1=booking["reservation_end"], col=secrets["flats"][flat]["cleaning_col"], offset=45106)
    response = google.write_to_cell(cell_range, value=str(booking["n_guests"]))


write_cleanings()
