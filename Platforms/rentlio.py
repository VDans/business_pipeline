import requests
import json
import pandas as pd

# secrets = json.load(open('../config_secrets.json'))
# API_KEY = secrets["rentlio"]["api_key"]


class Rentlio:
    def __init__(self, api_key):
        self.api_key = api_key

    def authenticate(self):
        """
        Authenticate to the Rentlio API.
        :return:
        """
        pass

    def get_data(self):
        response = requests.get(url=f"https://api.rentl.io/v1/reservations?apikey={self.api_key}")
        df = pd.json_normalize(json.loads(response.content)["data"])

        return df

    def close_date_range(self, platform):
        """
        See: https://docs.rentl.io/#unit-types-update-availability-and-rates-for-unit-type-post
        :param platform: Where to update the availability
        :return: None
        """
        pass

    def open_date_range(self, platform):
        pass
