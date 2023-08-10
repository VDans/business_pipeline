import json
from google_api import Google

from message_scrapers.airbnb_message import AirbnbCom

secrets = json.load(open('config_secrets.json'))

# a = AirbnbCom(secrets)
# a.send_message(thread_id="1567680309", image_path="Resources/OMG10/OMG CHECKIN.png")
g = Google(secrets=secrets, workbook_id=secrets["google"]["pricing_workbook_id"])

{
    "updateCells": {
        "rows": [
            {
                "values": [{
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 1,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1
                        }}}
                ]
            }
        ],
        "fields": 'userEnteredFormat.backgroundColor',
        "range": {
            "sheetId": sheetId,
            "startRowIndex": 0,
            "endRowIndex": 1,
            "startColumnIndex": 0,
            "endColumnIndex": 1
        }
    }
}