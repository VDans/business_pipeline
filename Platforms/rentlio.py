import requests
import json
import pandas as pd
from datetime import datetime


class Rentlio:
    def __init__(self, api_key, resources):
        self.api_key = "V5wOxl51lOMUe9vanleqNZjdLWcjjiZg"
        self.resources = resources

        self.bookings = None

    def authenticate(self):
        """
        Authenticate to the Rentlio API.
        :return:
        """
        pass

    def get_clean_bookings(self):
        response = requests.get(url=f"https://api.rentl.io/v1/reservations?apikey={self.api_key}")
        self.bookings = pd.json_normalize(json.loads(response.content)["data"])

        self.map_columns()
        self.filter_columns()
        self.prepare_columns()

    def map_columns(self):
        self.bookings.columns = [self.resources["columns_in"][c] for c in self.bookings.columns]

    def filter_columns(self):
        self.bookings = self.bookings[self.resources["columns_out"].keys()]

    def prepare_columns(self):
        # status 1 OK, 5 canceled
        # Add cleaning fee in services
        # Dates to datetime
        self.bookings["check_in_date"] = [datetime.fromtimestamp(int(c)) for c in self.bookings["check_in_date"]]
        self.bookings["check_out_date"] = [datetime.fromtimestamp(int(c)) for c in self.bookings["check_out_date"]]
        self.bookings["booking_date"] = [datetime.fromtimestamp(int(c)) for c in self.bookings["booking_date"]]
        self.bookings["status"] = ["canceled" if c == 5 else "ok" for c in self.bookings["status"]]
        self.bookings["guest_name"] = [n.title() for n in self.bookings["guest_name"]]
