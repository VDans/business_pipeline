import requests
import json
import pandas as pd

from requests.auth import HTTPBasicAuth

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))

P_ID = 435307

url = "https://api.lodgify.com/v1/reservation"

headers = {"accept": "application/json", "X-ApiKey": secrets["lodgify"]["api_key"]}

response = requests.get(url, headers=headers)
bookings = pd.json_normalize(json.loads(response.content)["items"])
print("")
