import requests
import json
import pandas as pd

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))

url = "https://login.smoobu.com/api/reservations"

P_ID = "1537828"
API_KEY = "amDUd156qFXFldmk9H5TYNy1rdIEDI6yXXyJOxryKw"

headers = {"accept": "application/json", "API-key": API_KEY}

response = requests.get(url,
                        headers=headers,
                        params={
                            "arrivalFrom": "2023-03-01"
                        })
print(response.text)
bookings = pd.json_normalize(json.loads(response.content)["bookings"]).sort_values(["arrival"])
print("")
