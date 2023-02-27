import requests
import json
import pandas as pd

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))

account = "1672745038.7538"
url = f"https://capi.tokeet.com/v1/guest?account={account}"
payload = {}
files = {}
headers = {
  'Authorization': 'f3fa9fe5-e700-4629-a7f9-19cc3f756801'
}
#eb539311-0eca-4ebb-abc3-c00a5f54e929
response = requests.request("GET", url, headers=headers, data=payload, files=files)

print(json.loads(response.content)["data"])
bookings = pd.json_normalize(json.loads(response.content)["data"])
print("")
