import requests
import json

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))


class Zodomus:
    def __init__(self):
        self.api_url = "https://api.zodomus.com/"
        self.api_key_user = secrets[""]
        self.api_key_password = ""

    def get_channels(self):
        api_call = self.api_url + "channels"
        r = requests.post('https://api.zodomus.com/channels', auth=HTTPBasicAuth(self.api_key_user, self.api_key_password))
