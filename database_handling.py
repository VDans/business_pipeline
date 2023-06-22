import pandas as pd


class DatabaseHandler:
    def __init__(self, db_engine, secrets):
        self.db_engine = db_engine
        self.secrets = secrets

    def upload_reservation(self, channel_id_z, unit_id_z, reservation_z):
        out = self.clean_reservation_z(channel_id_z, unit_id_z, reservation_z)
        out.to_sql(
            index=False,
            con=self.db_engine,
            name='bookings',
            if_exists='append'
        )

    def clean_reservation_z(self, channel_id_z, unit_id_z, reservation_z):
        data = reservation_z["reservations"]
        out = pd.DataFrame([{
            "booking_id": data["reservation"]["id"],
            "booking_date": data["reservation"]["bookedAt"],
            "object": self.secrets["flat_names"][unit_id_z],
            "reservation_start": pd.Timestamp(data["rooms"][0]["arrivalDate"]),
            "reservation_end": pd.Timestamp(data["rooms"][0]["departureDate"]),
            "status": "OK",
            "guest_name": f"""{data["customer"]["firstName"]} {data["customer"]["lastName"]}""",
            "guest_origin": f"""{data["customer"]["address"]} {data["customer"]["city"]} {data["customer"]["zipCode"]} {data["customer"]["countryCode"]}""",
            "adults": data["rooms"][0]["numberOfGuests"],  # Can we trust "numberOfAdults"?
            "children": data["rooms"][0]["numberOChildren"],
            "platform": "Booking.com" if channel_id_z == "1" else "Airbnb",
            "nights_price": data["rooms"][0]["totalPrice"],
            "cleaning": None,
            "extras": None,
            "discount": None,
            "commission_guest": None,
            "commission_host": None,
            "payment_commission": None,
            "phone": data["customer"]["phone"],
            "email": data["customer"]["email"]
        }])
        return out
