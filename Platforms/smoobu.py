import requests
import json
import pandas as pd


class Smoobu:
    def __init__(self, secrets, resources):
        self.secrets = secrets
        self.resources = resources
        self.url = "https://login.smoobu.com/api"

        self.headers = {"accept": "application/json", "API-key": self.secrets["smoobu"]["api_key"]}

    def get_smoobu_bookings(self, from_date, to_date, unit_id, filter_by="arrival"):
        """
        Attention! Not including a from and to_date might result in incomplete results!
        :return: Table with Smoobu reservations
        """
        unit_id: int = self.resources["smoobu_properties"][unit_id]
        route = "/reservations"
        parameters = None

        if filter_by == "check-in":
            parameters = {
                "arrivalFrom": from_date.strftime("%Y-%m-%d"),
                "arrivalTo": to_date.strftime("%Y-%m-%d"),
                "apartmentId": unit_id
            }
        elif filter_by == "check-out":
            parameters = {
                "departureFrom": from_date.strftime("%Y-%m-%d"),
                "departureTo": to_date.strftime("%Y-%m-%d"),
                "apartmentId": unit_id
            }
        else:
            ValueError("filter not recognized. Choose from 'check-in' and 'check-out'.")

        resp = requests.get(url=self.url + route,
                            headers=self.headers,
                            params=parameters)
        out = pd.json_normalize(json.loads(resp.content)["bookings"])

        if len(out) > 0:
            out = out.sort_values(["arrival"])

        return out

    def update_booking(self, booking_id, **updates):
        route = f"/reservations/{booking_id}"

        parameters = {
            "adults": updates["adults"],
            "children": updates["children"]
        }
        resp = requests.post(url=self.url + route,
                             headers=self.headers,
                             params=parameters)
        return resp
