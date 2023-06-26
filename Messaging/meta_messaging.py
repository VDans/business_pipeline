import requests as req
import json

secrets = json.load(open('../config_secrets.json'))


class Whatsapp:
    def __init__(self):
        self.base_url = 'https://graph.facebook.com/v14.0/'

        self.business_id = secrets['whatsapp']['Business ID']
        self.app_id = secrets['whatsapp']['App ID']
        self.app_secret = secrets['whatsapp']['App Secret']
        self.temporary_access_token = secrets['whatsapp']['Temporary Access Token']
        self.phone_number_id = secrets['whatsapp']['Phone Number ID']
        self.whatsapp_business_account_id = secrets['whatsapp']['Whatsapp Business Account ID']

        self.request_header = ''
        self.get_header()

    def get_header(self):
        self.request_header = {"Authorization": "Bearer " + self.temporary_access_token,
                               "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly",
                               "ConsistencyLevel": "eventual",
                               'Content-Type': 'application/json'}

    def send_whatsapp_message(self):
        """
        { "template": { "name": "hello_world", \"language\": { \"code\": \"en_US\" } } }
        """
        message_url = 'https://graph.facebook.com/v14.0/104862559091964/messages'

        dat = {
            "messaging_product": "whatsapp",
            # "recipient_type": "individual",
            "to": "4368110286173",
            "type": "template",
            "template": {
                "name": "new_cleaning_needed",
                "language": {
                    "code": "en"
                },
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {
                                "type": "text",
                                "text": "Ilian"
                            },
                            {
                                "type": "text",
                                "text": "22nd of November 2022"
                            }
                        ]
                    }
                ]
            }
        }

        response = req.post(headers=self.request_header,
                            url=message_url,
                            json=dat)
        return response.text

# wm = WhatsappMessenger()
# wm.get_header()
# print(wm.send_messages())
