import json
from zodomus_api import Zodomus
secrets = json.load(open('config_secrets.json'))

z = Zodomus(secrets)
