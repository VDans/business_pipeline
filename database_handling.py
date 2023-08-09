import logging

import pandas as pd


class DatabaseHandler:
    def __init__(self, db_engine, secrets):
        self.db_engine = db_engine
        self.secrets = secrets

        self.curs = self.db_engine.raw_connection().cursor()

    def upload_reservation(self, channel_id_z, flat_name, reservation_z):
        logging.info(f"Starting reservation upload")
        out = self.clean_reservation_z(channel_id_z, flat_name, reservation_z)

        # Check if the booking_id is already in the DB with the status 'OK':
        duplicate = self.query_data(f"SELECT booking_id FROM bookings WHERE status = 'OK' AND booking_id = '{reservation_z['reservations']['reservation']['id']}'")
        if len(duplicate["booking_id"]) == 0:
            out.to_sql(
                index=False,
                con=self.db_engine,
                name='bookings',
                if_exists='append'
            )
        else:
            logging.warning(f"This booking_id is already on the DB with status OK!")

    def clean_reservation_z(self, channel_id_z, flat_name, reservation_z):
        data = reservation_z["reservations"]

        # Booking ID
        try:
            booking_id = data["reservation"]["id"]
        except Exception as ex:
            booking_id = None
            logging.error(f"Could not find booking ID with exception: {ex}")

        # Booking Date
        try:
            booking_date = data["reservation"]["bookedAt"]
        except Exception as ex:
            booking_date = None
            logging.error(f"Could not find booking_date with exception: {ex}")

        # Reservation Dates
        try:
            reservation_start = pd.Timestamp(data["rooms"][0]["arrivalDate"])
            reservation_end = pd.Timestamp(data["rooms"][0]["departureDate"])
        except Exception as ex:
            reservation_start = None
            reservation_end = None
            logging.error(f"Could not find reservation dates with exception: {ex}")

        # Guest Name
        try:
            guest_name = f"""{data["customer"]["firstName"]} {data["customer"]["lastName"]}"""
            guest_name = guest_name.title()
        except Exception as ex:
            guest_name = None
            logging.error(f"Could not find guest_name with exception: {ex}")

        # Guest Origin
        try:
            guest_origin = f"""{data["customer"]["address"]} {data["customer"]["city"]} {data["customer"]["zipCode"]} {data["customer"]["countryCode"]}"""
        except Exception as ex:
            guest_origin = None
            logging.error(f"Could not find guest_origin with exception: {ex}")

        # Number and type of guests
        try:
            adults = 0
            children = 0
            guests = data["rooms"][0]["guestCount"]  # List of dicts
            for g in guests:
                if g["adult"] == 1:
                    adults += int(g["count"])
                else:
                    children += int(g["count"])
        except Exception as ex:
            adults = None
            children = None
            logging.error(f"Could not find number of guests with exception: {ex}")

        # Nights Price
        try:
            nights_price = float(data["reservation"]["totalPrice"])
        except Exception as ex:
            nights_price = None
            logging.error(f"Could not find nights_price with exception: {ex}")

        # Cleaning Fee
        cleaning_fee = self.extract_cleaning_fee(channel_id_z=channel_id_z,
                                                 reservation_z=reservation_z,
                                                 flat_name=flat_name)

        # Commission Guest
        try:
            commission_guest = self.extract_commission_guest(channel_id_z, reservation_z)
        except Exception as ex:
            commission_guest = None
            logging.error(f"Could not find commission_guest with exception: {ex}")

        # Commission Host
        try:
            commission_host = self.extract_commission_host(channel_id_z, reservation_z, cleaning_fee)
        except Exception as ex:
            commission_host = None
            logging.error(f"Could not find commission_host with exception: {ex}")

        # Phone
        try:
            phone = data["customer"]["phone"].replace(" ", "")
        except Exception as ex:
            phone = None
            logging.error(f"Could not find phone with exception: {ex}")

        # Email
        try:
            email = data["customer"]["email"].replace(" ", "")
        except Exception as ex:
            email = None
            logging.error(f"Could not find email with exception: {ex}")

        # Thread ID (Airbnb):
        try:
            if str(channel_id_z) == "3":
                thread_id = reservation_z["fullResponse"]["threadId"]
            else:
                logging.info(f"Could not find thread_id because this is a Booking.com reservation.")
                thread_id = None
        except Exception as ex:
            thread_id = None
            logging.error(f"Could not find thread_id with exception: {ex}")

        out = pd.DataFrame([{
            "booking_date": booking_date,
            "object": flat_name,
            "reservation_start": reservation_start,
            "reservation_end": reservation_end,
            "status": "OK",
            "guest_name": guest_name,
            "guest_origin": guest_origin,
            "adults": adults,
            "children": children,
            "platform": "Booking" if str(channel_id_z) == "1" else "Airbnb",
            "nights_price": nights_price,
            "cleaning": cleaning_fee,
            "commission_guest": commission_guest,
            "commission_host": commission_host,
            "phone": phone,
            "email": email,
            "booking_id": booking_id,
            "thread_id": thread_id
        }])
        return out

    def query_data(self, sql: str, data=None, dtypes=None):

        self.curs.execute(sql,  data)
        col_names = [i[0].lower() for i in self.curs.description]
        df = self.curs.fetchall()

        df = pd.DataFrame(data=df,
                          columns=col_names)

        if dtypes:
            df = self.force_dtypes(df, dtypes)

        return df

    logging.info(f"Extracting price information from reservation data")

    @staticmethod
    def extract_commission_host(channel_id_z, reservation_z, cleaning_fee):
        """INCLUDES PAYMENT COMMISSION FOR BOOKING.COM!"""
        logging.info(f"Extracting host commission from reservation data")
        try:
            if str(channel_id_z) == "1":
                commission = (-1) * 0.162 * (float(reservation_z["reservations"]["reservation"]["totalPrice"]) + cleaning_fee)
            else:
                commission = (-1) * (float(reservation_z["fullResponse"]["hostFeeBaseAccurate"]) + float(reservation_z["fullResponse"]["hostFeeVatAccurate"]))
        except KeyError as ke:
            logging.error(f"Error in finding guest commission with error: {ke}. Moving on with 0")
            commission = 0

        return commission

    @staticmethod
    def extract_commission_guest(channel_id_z, reservation_z):
        logging.info(f"Extracting guest commission from reservation data")
        try:
            if str(channel_id_z) == "1":
                commission = 0
            else:
                commission = float(reservation_z["fullResponse"]["guestFeeBaseAccurate"]) + float(reservation_z["fullResponse"]["guestFeeVatAccurate"])
        except KeyError as ke:
            logging.error(f"Error in finding guest commission with error: {ke}. Moving on with 0")
            commission = 0
        return commission

    @staticmethod
    def force_dtypes(df: pd.DataFrame, dtypes: dict = None):
        # Find the keys in dtypes where the value is "Timestamp" or "datetime", and convert those, while removing them from the dtypes dict, to transmit to astype()
        date_type_keys: list = [k for k, v in dtypes.items() if "Timestamp" in str(v) or "datetime" in str(v)]  # Einstein?
        for c in date_type_keys:
            df[c] = pd.to_datetime(df[c])
            dtypes.pop(c)

        # Convert now the rest
        df = df.astype(dtypes)

        return df

    def close_engine(self):
        self.db_engine.dispose()

    @staticmethod
    def extract_cleaning_fee(channel_id_z, reservation_z, flat_name):
        logging.info(f"Extracting cleaning fee from reservation data")
        try:
            if str(channel_id_z) == "1":
                fees = reservation_z["reservations"]["rooms"][0]["priceDetailsExtra"]  # List of extra fees
                cleaning_fee = 0
                for f in fees:
                    if (f["text"] == "Cleaning fee") & (f["included"] == "no"):
                        cleaning_fee += int(float(f["amount"]))
            else:
                cleaning_fee = int(float(reservation_z["fullResponse"]["listingCleaningFeeAccurate"]))
        except KeyError as ke:
            logging.error(f"Error in finding fees for flat {flat_name}, with error: {ke}. Moving on with fees = 0")
            cleaning_fee = 0

        return cleaning_fee
