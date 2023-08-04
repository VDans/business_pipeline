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
        out.to_sql(
            index=False,
            con=self.db_engine,
            name='bookings',
            if_exists='append'
        )

    def clean_reservation_z(self, channel_id_z, flat_name, reservation_z):
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

        cleaning_fee = self.extract_cleaning_fee(channel_id_z=channel_id_z, reservation_z=reservation_z, flat_name=flat_name)

        out = pd.DataFrame([{
            "booking_id": data["reservation"]["id"],
            "booking_date": data["reservation"]["bookedAt"],
            "object": flat_name,
            "reservation_start": pd.Timestamp(data["rooms"][0]["arrivalDate"]),
            "reservation_end": pd.Timestamp(data["rooms"][0]["departureDate"]),
            "status": "OK",
            "guest_name": f"""{data["customer"]["firstName"]} {data["customer"]["lastName"]}""",
            "guest_origin": f"""{data["customer"]["address"]} {data["customer"]["city"]} {data["customer"]["zipCode"]} {data["customer"]["countryCode"]}""",
            "adults": adults,
            "children": children,
            "platform": "Booking" if str(channel_id_z) == "1" else "Airbnb",
            "nights_price": float(data["rooms"][0]["totalPrice"]),
            "cleaning": cleaning_fee,
            "extras": None,  # Can't know until I've processed an Airbnb Reservation. None in Booking.com.
            "discount": None,  # Can't know until I've processed an Airbnb Reservation. None in Booking.com.
            "commission_guest": None,  # Can't know until I've processed an Airbnb Reservation. None in booking.com.
            "commission_host": - 0.15 * (float(data["rooms"][0]["totalPrice"]) + cleaning_fee),
            "payment_commission": - 0.012 * (float(data["rooms"][0]["totalPrice"]) + cleaning_fee),
            "phone": data["customer"]["phone"].replace(" ", ""),
            "email": data["customer"]["email"].replace(" ", "")
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

    def extract_cleaning_fee(self, channel_id_z, reservation_z, flat_name):
        logging.info(f"Extracting cleaning fee from reservation data")
        try:
            if str(channel_id_z == "1"):
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
