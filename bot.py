import json
import logging
import time

import pandas as pd
from flask import Flask, request
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus
from google_api import Google

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
secrets = json.load(open('config_secrets.json'))


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return str("Welcome to the Pricing Web App of Host-It Immobilien GmbH")


@app.route('/availability', methods=['POST'])
def manage_availability():
    """
    Zodomus only sends a notification that a new/changed/cancelled reservation happens.
    1. Get payload from Zodomus
    2. GET /reservations with the reservation number from the webhook
    3. Using the dates and property retrieved in (.2), close dates using the API call.
    """
    start_time = time.time()  # Checking the length of each request
    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("AVAILABILITY New Request------------------------------------------------------------------------")

    try:
        reservation_status_z = data["reservationStatus"]
        logging.info(f"Retrieved the reservationStatus: {reservation_status_z}")
        try:
            if str(data["channelId"]) == '1':
                flat_name = [fn for fn in secrets['flats'] if (secrets["flats"][fn]["pid_booking"] == data["propertyId"]) or (data["propertyId"] in secrets["flats"][fn]["older_pid"])][0]
            else:
                flat_name = [fn for fn in secrets['flats'] if (secrets["flats"][fn]["pid_airbnb"] == data["propertyId"]) or (data["propertyId"] in secrets["flats"][fn]["older_pid"])][0]
        except Exception as e:
            flat_name = "UNKNOWN"
            logging.error(f"Could NOT retrieve the flat name: {e}")

        channel_name = "Airbnb" if str(data["channelId"]) == "3" else "Booking"

        z = Zodomus(secrets=secrets)
        g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])
        db_engine = create_engine(url=secrets["database"]["url"])
        dbh = DatabaseHandler(db_engine, secrets)
        offset = g.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15)) - 3  # Rolling window. -3 for 3 headers rows!

        # Find the reservation
        reservation_z = z.get_reservation(channel_id=data["channelId"], unit_id_z=data["propertyId"], reservation_number=data["reservationId"]).json()
        logging.info("Retrieved reservation data")

        # Initiate bookings table
        tbl = Table("bookings", MetaData(), autoload_with=db_engine)

        logging.info(f"Step 1 finishes at timestamp {time.time() - start_time} seconds.")

        if reservation_status_z == '1':  # New
            # Exception at GBS... If other exceptions appear, change pid/rid logic.
            if (flat_name == "GBS") and (channel_name == "Booking"):
                flat_name = [fn for fn in secrets['flats'] if secrets["flats"][fn]["rid_booking"] == reservation_z["reservations"]["rooms"][0]["id"]][0]

            logging.info(f"New booking in {flat_name} from {reservation_z['reservations']['rooms'][0]['arrivalDate']} to {reservation_z['reservations']['rooms'][0]['departureDate']}")

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
            z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=date_from, date_to=date_to, availability=0)
            z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=date_from, date_to=date_to+pd.Timedelta(days=-1), availability=0)
            logging.info("Availability has been closed in both channels")

            logging.info(f"Step 2 finishes at timestamp {time.time() - start_time} seconds.")

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
            logging.info(f"Step 3 finishes at timestamp {time.time() - start_time} seconds.")

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
            logging.info(f"Step 4 finishes at timestamp {time.time() - start_time} seconds.")

            try:
                cleaning_fee = dbh.extract_cleaning_fee(channel_id_z=str(data["channelId"]), reservation_z=reservation_z, flat_name=flat_name)
                total_price = float(reservation_z["reservations"]["rooms"][0]["totalPrice"]) + cleaning_fee
                duration = (date_to - date_from).days
                note = f"""{reservation_z["reservations"]["customer"]["firstName"].title()} {reservation_z["reservations"]["customer"]["lastName"].title()}\nPaid {total_price}€\nGuests: {n_guests}\nNights: {duration}\nFrom {date_from.strftime("%d.%m")} To {date_to.strftime("%d.%m")}\nID: {data["reservationId"]}"""
                g.write_note2(date_from, date_from, flat_name, note=note, offset=offset)
            except Exception as ex:
                logging.error(f"Could not write note! Exception: {ex}")

            logging.info(f"Wrote '{channel_name}' within the pricing Google Sheet. Added info note.")
            logging.info(f"Step 5 finishes at timestamp {time.time() - start_time} seconds.")

        elif reservation_status_z == '2':  # Modified
            # Exception at GBS... If other exceptions appear, change pid/rid logic.
            if (flat_name == "GBS") and (channel_name == "Booking"):
                flat_name = [fn for fn in secrets['flats'] if secrets["flats"][fn]["rid_booking"] == reservation_z["reservations"]["rooms"][0]["id"]][0]

            logging.info(f"Modified booking in {flat_name}")

            try:
                # Update OLD reservation data DB status to 'Modified'
                upd = update(tbl).where(tbl.c.booking_id == str(data['reservationId'])).values(status="Modified")
                with db_engine.begin() as conn:
                    conn.execute(upd)
                    logging.info(f"UPDATE bookings SET status = 'Modified' WHERE booking_id = '{data['reservationId']}'")

                # Upload NEW reservation data to DB
                dbh.upload_reservation(channel_id_z=data["channelId"], flat_name=flat_name, reservation_z=reservation_z)
                logging.info('Reservation uploaded to table "bookings"')

                # Get OLD dates, and open them:
                old_dates = dbh.query_data(f"SELECT reservation_start, reservation_end FROM bookings WHERE status = 'Modified' AND booking_id = '{data['reservationId']}'")
                old_date_from = pd.Timestamp(old_dates["reservation_start"][0])
                old_date_to = pd.Timestamp(old_dates["reservation_end"][0])
                z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=old_date_from, date_to=old_date_to, availability=1)
                z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=old_date_from, date_to=old_date_to+pd.Timedelta(days=-1), availability=1)
                logging.info("Old dates have been opened in both channels")

                # Get NEW dates, and close them:
                new_date_from = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["arrivalDate"])
                new_date_to = pd.Timestamp(reservation_z["reservations"]["rooms"][0]["departureDate"])
                z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=new_date_from, date_to=new_date_to, availability=0)
                z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=new_date_from, date_to=new_date_to+pd.Timedelta(days=-1), availability=0)
                logging.info("New dates have been closed in both channels")

                logging.info(f"Step 2 finishes at timestamp {time.time() - start_time} seconds.")

                # Unmerge the cells based on the first one:
                try:
                    g.unmerge_cells2(old_date_from, old_date_to, flat_name, offset=offset)
                    g.write_note2(old_date_from, old_date_from, flat_name, "", offset=offset)
                except Exception as ex:
                    logging.warning(f"Could not unmerge and remove the note: {ex}")

                # Remove the "Booked" in the Google Sheet and replace with 4 nights by default
                try:
                    dates_range = pd.Series(pd.date_range(start=old_date_from, end=(old_date_to - pd.Timedelta(days=1))))
                    dat = []
                    dates_range.apply(add_write_snippet, args=(g, dat, flat_name, 4))
                    g.batch_write_to_cell(data=dat)
                except Exception as ex:
                    logging.warning(f"Could not write to sheet: {ex}")

                logging.info("Removed the booking tag within the pricing Google Sheet. Overwrote the note.")

                # Write the "Booked" in the Google Sheet
                n_guests = get_n_guests(reservation_z)

                try:
                    # Write the name of the guest:
                    try:
                        part2 = " " + reservation_z["reservations"]["customer"]["lastName"].title()[0] + "."
                    except IndexError as ie:
                        part2 = ""
                    short_name = f"""{reservation_z["reservations"]["customer"]["firstName"].title()}{part2} ({channel_name[0]})"""
                    dates_range = pd.Series(pd.date_range(start=new_date_from, end=(new_date_to+pd.Timedelta(days=-1))))
                    dat = []
                    dates_range.apply(add_write_snippet, args=(g, dat, flat_name, short_name))
                    g.batch_write_to_cell(data=dat)

                except Exception as ex:
                    logging.warning(f"Could not write to sheet: {ex}")

                try:
                    g.merge_cells2(new_date_from, new_date_to, flat_name, offset=offset)
                except Exception as ex:
                    logging.error(f"Could not merge cells with exception: {ex}")

                try:
                    cleaning_fee = dbh.extract_cleaning_fee(channel_id_z=str(data["channelId"]), reservation_z=reservation_z, flat_name=flat_name)
                    total_price = float(reservation_z["reservations"]["rooms"][0]["totalPrice"]) + cleaning_fee
                    duration = (new_date_to - new_date_from).days
                    note = f"""{reservation_z["reservations"]["customer"]["firstName"].title()} {reservation_z["reservations"]["customer"]["lastName"].title()}\nPaid {total_price}€\nGuests: {n_guests}\nNights: {duration}\nFrom {new_date_from.strftime("%d.%m")} To {new_date_to.strftime("%d.%m")}\nID: {data["reservationId"]}"""
                    g.write_note2(new_date_from, new_date_from, flat_name, note=note, offset=offset)

                except Exception as ex:
                    logging.error(f"Could not write note! Exception: {ex}")

                logging.info(f"Wrote '{channel_name}' within the pricing Google Sheet. Added info note.")

            except KeyError as ke:
                logging.error(f"ERROR in the processing of the modification: {ke}")
                # body = f"ERROR: Could not process modification"

        elif reservation_status_z == '3':  # Cancelled
            # Exception at GBS... If other exceptions appear, change pid/rid logic.
            if (flat_name == "GBS") and (channel_name == "Booking"):
                flat_name = dbh.query_data(f"SELECT object FROM bookings WHERE status = 'OK' AND booking_id = '{data['reservationId']}'")["object"][0]

            logging.info(f"Cancelled booking in {flat_name}")
            try:
                with db_engine.begin() as conn:
                    try:
                        # Set Status to Cancelled
                        upd1 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(status="Cancelled")
                        conn.execute(upd1)
                        logging.info(f"UPDATE bookings SET status = 'Cancelled' WHERE booking_id = '{data['reservationId']}'")
                    except Exception as ex:
                        logging.error(f"Couldn't update status: {ex}")

                    try:
                        # Set cleaning fee to 0
                        upd2 = update(tbl).where(tbl.c.booking_id == str(data['reservationId']), tbl.c.status == "OK").values(cleaning=0)
                        conn.execute(upd2)
                        logging.info(f"UPDATE bookings SET cleaning = 0 WHERE booking_id = '{data['reservationId']}'")
                    except Exception as ex:
                        logging.error(f"Couldn't update cleaning fee: {ex}")

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

                # Have to get the dates from the DB because not provided by
                dates = dbh.query_data(f"SELECT reservation_start, reservation_end FROM bookings WHERE booking_id = '{data['reservationId']}'")
                date_from = pd.Timestamp(dates["reservation_start"][0])
                date_to = pd.Timestamp(dates["reservation_end"][0])

                z.set_availability(channel_id="1", unit_id_z=secrets["flats"][flat_name]["pid_booking"], room_id_z=secrets["flats"][flat_name]["rid_booking"], date_from=date_from, date_to=date_to, availability=1)
                z.set_availability(channel_id="3", unit_id_z=secrets["flats"][flat_name]["pid_airbnb"], room_id_z=secrets["flats"][flat_name]["rid_airbnb"], date_from=date_from, date_to=date_to+pd.Timedelta(days=-1), availability=1)
                logging.info("Availability has been opened in both channels")

                # Unmerge the cells based on the first one:
                try:
                    g.unmerge_cells2(date_from, date_to, flat_name, offset=offset)
                    g.write_note2(date_from, date_to, flat_name, "", offset=offset)
                    logging.info("Removed the booking tag within the pricing Google Sheet. Overwrote the note.")

                except Exception as ex:
                    logging.warning(f"Could not unmerge and remove the note: {ex}")

                # Remove the "Booked" in the Google Sheet and replace with 4 nights by default
                try:
                    dates_range = pd.Series(pd.date_range(start=date_from, end=(date_to - pd.Timedelta(days=1))))
                    dat = []
                    dates_range.apply(add_write_snippet, args=(g, dat, flat_name, 4))
                    g.batch_write_to_cell(data=dat)
                except Exception as ex:
                    logging.warning(f"Could not write to sheet: {ex}")

            except KeyError as ke:
                logging.error(f"ERROR in the processing of the cancellation: {ke}")

        else:
            logging.error(f"reservationStatus not understood: {reservation_status_z}")

        dbh.close_engine()

    except Exception as e:
        logging.error(f"Couldn't find the 'ReservationStatus' in the request: {e}")
        logging.info(f"The request says: {data}")

    end_time = time.time()
    logging.info(f"This call lasted {end_time - start_time} seconds")

    return str("All Good!")


@app.route('/pricing', methods=['POST'])
def get_prices():
    """
    This url is called by the Google Webhook when a change occurs in the pricing Google Sheet.
    """
    start_time = time.time()

    data = request.json
    logging.info("--------------------------------------------------------------------------------------------------------")
    logging.info("PRICING New Request-------------------------------------------------------------------------------------")

    z = Zodomus(secrets=secrets)

    try:
        # Find out the booking and airbnb propertyId
        flat_name = data["flat_name"]
        property_id_airbnb = secrets["flats"][flat_name]["pid_airbnb"]
        room_id_airbnb = secrets["flats"][flat_name]["rid_airbnb"]
        rate_id_airbnb = secrets["flats"][flat_name]["rtid_airbnb"]
        property_id_booking = secrets["flats"][flat_name]["pid_booking"]
        room_id_booking = secrets["flats"][flat_name]["rid_booking"]
        rate_id_booking = secrets["flats"][flat_name]["rtid_booking"]

    except KeyError:
        logging.error(f"Could not find flat name: {data['flat_name']}, is it possible you're adding columns?")

        return str("Thanks Google.")

    for i in range(len(data["new_value"])):
        try:
            # Clean Date and Value
            date = pd.Timestamp(data["date"][i][0])
            value = int(data["new_value"][i][0])  # Price and min nights as integers. No real need for decimals...

            logging.info(f"Extracting data: Property: {data['flat_name']} - Date: {date.strftime('%Y-%m-%d')} - {data['value_type']}: {value}")

            # Pushing data through Zodomus:
            if data["value_type"] == "Price":
                logging.info(f"Modifying price: Pushing to channels")
                response1 = z.set_rate(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, price=value)
                response2 = z.set_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, price=value)

            elif data["value_type"] == "Min.":
                if str(value) == "0":
                    # 3. If min_nights = 0: Close the room for the night in both channels
                    logging.info("Min. Nights set to 0. Closing the room.")
                    z.set_availability(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, date_from=date, date_to=(date + pd.Timedelta(days=1)), availability=0)
                    z.set_availability(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, date_from=date, date_to=date, availability=0)

                else:
                    logging.info(f"Making sure availability is open before pushing min. nights value")
                    # 1. Make sure the dates are open. Why? Because if min nights was on 0, and you change the min nights, the nights stay closed.
                    z.set_availability(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, date_from=date, date_to=(date + pd.Timedelta(days=1)), availability=1)
                    z.set_availability(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, date_from=date, date_to=date, availability=1)

                    # 2. Change the minimum nights on the platforms
                    # UNFORTUNATELY the shitty Airbnb API requires a price push at the same time as the minimum nights' push.
                    # Therefore, you also have to communicate the price next to the min nights requirements...
                    try:
                        right_cell_value = int(data["rightCellValue"][i][0])
                    except Exception as ex:
                        right_cell_value = 500
                        logging.warning(f"No price is available! Setting price to 500 while waiting for a better price: {ex}")

                    logging.info(f"Pushing min. nights value")
                    z.set_minimum_nights(channel_id="1", unit_id_z=property_id_booking, room_id_z=room_id_booking, rate_id_z=rate_id_booking, date_from=date, min_nights=value)
                    z.set_airbnb_rate(channel_id="3", unit_id_z=property_id_airbnb, room_id_z=room_id_airbnb, rate_id_z=rate_id_airbnb, date_from=date, price=right_cell_value, min_nights=value)  # Fucking hate this...

            else:
                response1 = response2 = "value_type data not one of 'Price' or 'Min.'"
                logging.warning(f"Response: {response1}")

        except ValueError as ve:
            if data["new_value"][i][0] in ["Booked", "Airbnb", "Booking.com", "Booking"]:
                logging.warning(f"New '{data['new_value'][i][0]}' value entered. Skipping the logic.")
            else:
                logging.warning(f"Value {data['new_value'][i][0]} entered is not a valid input. Skipping the logic: {ve}")

    end_time = time.time()
    logging.info(f"This took {end_time - start_time} seconds")

    return str("Thanks Google!")


@app.route('/online-checkin', methods=['POST'])
def check_in_online():
    """
    Receive webhook from TypeForm Website with needed data.
    This call also triggers the expedition of the check-in instructions to the
    """
    start_time = time.time()

    data = request.json
    logging.info("------------------------------------------------------------------------------------------------")
    logging.info("OCI New Request---------------------------------------------------------------------------------")

    fa = data["form_response"]["answers"]
    logging.info(f"New online check-in submitted. Uploading to DB...")

    db_engine = create_engine(url=secrets["database"]["url"])
    dbh = DatabaseHandler(db_engine, secrets)

    # The first step is to identify the booking.
    try:
        booking_id_json = list(filter(lambda x: x["field"]["id"] == "aYx6wPeuUVQB", fa))
        booking_id = booking_id_json[0]["text"]
    except Exception as e:
        booking_id = None
        logging.error(f"Could not find booking_id with error: {e}")

    try:
        complete_name_json = list(filter(lambda x: x["field"]["id"] == "2jL0fJRRhvIx", fa))
        complete_name = complete_name_json[0]["text"]
    except Exception as e:
        complete_name = None
        logging.error(f"Could not find Complete Name with error: {e}")

    try:
        birth_date_json = list(filter(lambda x: x["field"]["id"] == "a0yIBjOxHLpz", fa))
        birth_date = pd.Timestamp(birth_date_json[0]["date"])
    except Exception as e:
        birth_date = pd.Timestamp(day=1, month=1, year=2050)
        logging.error(f"Could not find Birth Date with error: {e}")

    try:
        nationality_json = list(filter(lambda x: x["field"]["id"] == "nG5ElmuMaUjI", fa))
        nationality = nationality_json[0]["choice"]["label"]
    except Exception as e:
        nationality = None
        logging.error(f"Could not find Nationality with error: {e}")

    try:
        address1_json = list(filter(lambda x: x["field"]["id"] == "iyagz93Mh58C", fa))

        try:
            address2_json = list(filter(lambda x: x["field"]["id"] == "tHxUHfdhaCXb", fa))
            address2 = address2_json[0]['text']
        except Exception as e:
            logging.info(f"Guest did not enter address line 2: {e}")
            address2 = ""

        address3_json = list(filter(lambda x: x["field"]["id"] == "ezg8z74NJOAv", fa))
        address4_json = list(filter(lambda x: x["field"]["id"] == "QmIqIUzZNNX0", fa))
        address5_json = list(filter(lambda x: x["field"]["id"] == "eSle7zYGph6M", fa))
        address = f"{address1_json[0]['text']} {address2} {address3_json[0]['text']} {address4_json[0]['text']} {address5_json[0]['text']}"
    except Exception as e:
        address = None
        logging.error(f"Could not find Address with error: {e}")

    try:
        country_json = list(filter(lambda x: x["field"]["id"] == "4nOmw728gGvk", fa))
        country = country_json[0]["text"]
    except Exception as e:
        country = None
        logging.error(f"Could not find country with error: {e}")

    try:
        email_address_json = list(filter(lambda x: x["field"]["id"] == "Ldc3ijn1LoNl", fa))
        email_address = email_address_json[0]["email"]
    except Exception as e:
        email_address = None
        logging.error(f"Could not find email_address with error: {e}")

    try:
        phone_number_json = list(filter(lambda x: x["field"]["id"] == "jLITNsgAaEzd", fa))
        phone_number = phone_number_json[0]["phone_number"].replace(" ", "")
    except Exception as e:
        phone_number = None
        logging.error(f"Could not find phone_number with error: {e}")

    try:
        id_type_json = list(filter(lambda x: x["field"]["id"] == "ju5jsKP93xpa", fa))
        id_type = id_type_json[0]["choice"]["label"]
    except Exception as e:
        id_type = None
        logging.error(f"Could not find id_type with error: {e}")

    try:
        eta_json = list(filter(lambda x: x["field"]["id"] == "0VQK8k56v3Tb", fa))
        eta = eta_json[0]["text"]
    except Exception as e:
        eta = None
        logging.error(f"Could not find ETA with error: {e}")

    try:
        beds_json = list(filter(lambda x: x["field"]["id"] == "dGapMiuCAuWX", fa))
        beds = beds_json[0]["text"]
    except Exception as e:
        beds = None
        logging.error(f"Guest did not enter beds preference: {e}")

    out = pd.DataFrame({
        "complete_name": [complete_name],
        "birth_date": [birth_date],
        "nationality": [nationality],
        "address": [address],
        "country": [country],
        "email_address": [email_address],
        "phone_number": [phone_number],
        "id_type": [id_type],
        "eta": [eta],
        "beds": [beds],
        "booking_id": [booking_id]
    })

    logging.info(f"Checking if this combination of booking_id and complete name is already on the database...")
    dbh.query_data(sql=f"SELECT * FROM checkin_data WHERE booking_id = '{booking_id}' AND complete_name = '{complete_name}';")

    try:
        out.to_sql(name="checkin_data",
                   con=db_engine,
                   if_exists="append",
                   index=False)
    except IntegrityError as ie:
        logging.error(f"A duplicate has been found, and the new data was not uploaded: {ie}")

    logging.info(f"Data uploaded to the DB with success: {complete_name}")

    # Find email corresponding to the booking_id:
    try:
        _out = dbh.query_data(sql=f"""SELECT email, object FROM bookings WHERE booking_id = '{booking_id}'""")

        if len(_out) > 0:
            # Get the language:
            language = 'german' if country in ['Austria', 'Germany'] else 'english'

            # Get the flat name:
            flat_name = _out["object"][0]

            # Get the email:
            recipient_email = _out["email"][0]

            # Get the message & language:
            _check_in_instructions = dbh.query_data(sql=f"""SELECT message FROM messages WHERE message_language = '{language}' and flat_name = '{flat_name}'""")
            if len(_check_in_instructions) > 0:
                check_in_instructions = _check_in_instructions["message"][0]
                logging.info(f"Checking instructions in {language} for flat {flat_name} found.")

                # Send the email:
                send_check_in_instructions(recipient_email=recipient_email, message=check_in_instructions)

            else:
                logging.info(f"Checking instructions in {language} for flat {flat_name} NOT found. Switching language...")
                language = 'english' if language == 'german' else 'german'
                _check_in_instructions = dbh.query_data(sql=f"""SELECT message FROM messages WHERE message_language = '{language}' and flat_name = '{flat_name}'""")

                if len(_check_in_instructions) > 0:
                    check_in_instructions = _check_in_instructions["message"][0]
                    logging.info(f"Checking instructions in {language} for flat {flat_name} found.")

                    # Send the email:
                    send_check_in_instructions(recipient_email=recipient_email, message=check_in_instructions)

                else:
                    logging.info(f"Checking instructions in {language} for flat {flat_name} NOT found. The instructions are not available in this flat...")

        else:
            send_check_in_instructions(recipient_email="office@host-it.at", message=f"The guest {complete_name} has given the comfirmation number {booking_id}, which hasn't been found in the database.\nTherefore, they have not received any instructions!\nPlease make sure the data is right, and send manually.")
            logging.warning(f"Could NOT find the booking_id {booking_id} given by the guest! Could it be from a property outside of the system")

    except Exception as e:
        logging.error(f"Could NOT even reach the querying using this booking_id: {e}")

    dbh.close_engine()

    end_time = time.time()
    logging.info(f"This took {end_time - start_time} seconds")

    return str("Thanks for checking in!")


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


def add_write_snippet(booking_date, google, data, flat, value):
    cell_range = google.get_rolling_range(unit_id=flat, date1=booking_date, col=secrets["flats"][flat]["pricing_col"], headers_rows=3)
    snippet = {
        "range": cell_range,
        "values": [
            [value]
        ]
    }
    data.append(snippet)


if __name__ == '__main__':
    app.run(debug=True)
