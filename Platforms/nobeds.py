import requests
import json
import pandas as pd

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))

account = "1672745038.7538"
url = f"https://api.nobeds.com/api/Airbnb/{secrets['nobeds']['api_token']}"
payload = {}
files = {}

response = requests.request("GET", url, data=payload, files=files)

print(json.loads(response.content)["data"])
bookings = pd.json_normalize(json.loads(response.content)["data"])
print("")
