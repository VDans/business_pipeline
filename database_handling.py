import logging
import pandas as pd


class DatabaseHandler:
    def __init__(self, db_engine, secrets):
        self.db_engine = db_engine
        self.secrets = secrets
        self.curs = self.db_engine.raw_connection().cursor()

    def upload_reservation(self, revision, room_position, booking_id):
        logging.info(f"Starting reservation upload")
        out = self.clean_reservation(revision, room_position, booking_id)

        # Check if the booking_id is already in the DB with the status 'OK':
        duplicate = self.query_data(f"SELECT booking_id FROM bookings WHERE status = 'OK' AND booking_id = '{booking_id}'")
        # Temporary freeze for the staging period:
#        if len(duplicate["booking_id"]) == 0:
#                out.to_sql(
#                index=False,
#                con=self.db_engine,
#                name='bookings',
#                if_exists='append'
#            )
#        else:
#            logging.warning(f"This booking_id is already on the DB with status OK!")

    def clean_reservation(self, revision, room_position, booking_id):
        data = revision["data"]
        room_id = data["attributes"]["rooms"][room_position]["room_type_id"]

        # Which flat corresponds to this room_id?
        try:
            flat_name = [fn for fn in self.secrets['flats'] if self.secrets["flats"][fn]["rid_channex"] == room_id][0]
        except Exception as ex:
            flat_name = "UNKNOWN"
            logging.error(f"Could not find flat_name with exception: {ex}")

        # Booking Date
        try:
            booking_date = pd.Timestamp.now()
        except Exception as ex:
            booking_date = None
            logging.error(f"Could not find booking_date with exception: {ex}")

        # Reservation Dates
        try:
            reservation_start = pd.Timestamp(data["attributes"]["arrival_date"]).date()
            reservation_end = pd.Timestamp(data["attributes"]["departure_date"]).date()
        except Exception as ex:
            reservation_start = None
            reservation_end = None
            logging.error(f"Could not find reservation dates with exception: {ex}")

        # Guest Name
        try:
            guest_name = f"""{data["attributes"]["customer"]["name"]} {data["attributes"]["customer"]["surname"]}"""
            guest_name = guest_name.title()
        except Exception as ex:
            guest_name = None
            logging.error(f"Could not find guest_name with exception: {ex}")

        # Guest Origin
        try:
            guest_origin = f"""{data["attributes"]["customer"]["address"]}, {data["attributes"]["customer"]["zip"]} {data["attributes"]["customer"]["city"]}, {data["attributes"]["customer"]["country"]}"""
        except Exception as ex:
            guest_origin = None
            logging.error(f"Could not find guest_origin with exception: {ex}")

        # Number and type of guests
        # Now this is room specific, not booking specific.
        try:
            adults = int(data["attributes"]["rooms"][room_position]["occupancy"]["adults"])
            children = int(data["attributes"]["rooms"][room_position]["occupancy"]["children"])
        except Exception as ex:
            adults = 1000
            children = 1000
            logging.error(f"Could not find number of guests with exception: {ex}")

        # Nights Price
        nights_price = self.extract_nights_price(revision, room_position)

        # Cleaning Fee
        cleaning_fee = self.extract_cleaning_fee(revision, room_position)

        # Commission Guest
        commission_guest = data["attributes"]["rooms"][room_position]["ota_commission"]  # FixMe: Find guest commission

        # Commission Host
        commission_host = data["attributes"]["rooms"][room_position]["ota_commission"]

        # Phone
        try:
            phone = data["attributes"]["customer"]["phone"].replace(" ", "")
            phone = "+" + phone if "+" not in phone else phone
        except Exception as ex:
            phone = None
            logging.error(f"Could not find phone with exception: {ex}")

        # Email
        try:
            email = data["attributes"]["customer"]["mail"]
        except Exception as ex:
            email = None
            logging.error(f"Could not find email with exception: {ex}")

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
            "platform": "",
            "nights_price": nights_price,
            "cleaning": cleaning_fee,
            "commission_guest": commission_guest,
            "commission_host": commission_host,
            "phone": phone,
            "email": email,
            "booking_id": booking_id
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

    @staticmethod
    def extract_nights_price(revision, room_position):
        logging.info(f"Extracting nights price from reservation data")
        try:
            days_breakdown = revision["data"]["attributes"]["rooms"][room_position]["meta"]["days_breakdown"]
            out = 0
            for dbd in days_breakdown:
                logging.info(f"Price on the {dbd['date']}: {dbd['amount']}")
                out += float(dbd["amount"])
            logging.info(f"Nights price after computation: {out}")
        except KeyError:
            out = 0

        return out

    @staticmethod
    def extract_cleaning_fee(revision, room_position):
        """This actually computes ALL EXTRA FEES to the nights' prices"""
        logging.info(f"Extracting cleaning fee from reservation data")
        try:
            fees_breakdown = revision["data"]["attributes"]["rooms"][room_position]["taxes"]
            out = 0
            for fbd in fees_breakdown:
                if not fbd["is_inclusive"]:
                    logging.info(f"Adding fee {fbd['name']} of {fbd['total_price']}")
                    out += float(fbd['total_price'])
            logging.info(f"Cleaning fee after computation: {out}")
        except KeyError:
            out = 0

        return out

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
