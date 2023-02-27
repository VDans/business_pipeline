import requests
import json
import logging
import pandas as pd


class Smoobu:
    def __init__(self, secrets, resources):
        self.secrets = secrets
        self.resources = resources
        self.url = "https://login.smoobu.com/api"

        self.headers = {"accept": "application/json", "API-key": self.secrets["smoobu"]["api_key"]}
        self.smoobu_logger = logging.getLogger(__name__)

    def get_webhook_infos(self, payload):
        self.smoobu_logger.info(payload)

    def get_smoobu_bookings(self, from_date: pd.Timestamp, to_date: pd.Timestamp, unit_id: str, filter_by="check-in"):
        """
        Attention! Not including a from and to_date might result in incomplete results!
        :return: Table with Smoobu reservations
        """
        self.smoobu_logger.info(f"""Getting bookings in {unit_id} for a {filter_by} from {from_date.strftime("%Y-%m-%d")} to {to_date.strftime("%Y-%m-%d")}""")
        smoobu_unit_id: int = self.resources["smoobu_properties"][unit_id]
        route = "/reservations"
        parameters = None

        if filter_by == "check-in":
            parameters = {
                "arrivalFrom": from_date.strftime("%Y-%m-%d"),
                "arrivalTo": to_date.strftime("%Y-%m-%d"),
                "apartmentId": smoobu_unit_id
            }
        elif filter_by == "check-out":
            parameters = {
                "departureFrom": from_date.strftime("%Y-%m-%d"),
                "departureTo": to_date.strftime("%Y-%m-%d"),
                "apartmentId": smoobu_unit_id
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


from sqlalchemy import create_engine

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))

logging.basicConfig(level=logging.INFO)
db_engine = create_engine(url=secrets['database']['url'])

s = Smoobu(secrets, resources)
out = s.get_smoobu_bookings(from_date=pd.Timestamp(day=1, month=1, year=2022),
                            to_date=pd.Timestamp(day=1, month=3, year=2023),
                            unit_id="GBS124")

print("")
