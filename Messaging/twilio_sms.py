import json
from twilio.rest import Client
from twilio.twiml.messaging_response import Body, Media, Message, MessagingResponse

secrets = json.load(open('../secrets.json'))


class SmsEngine:
    def __init__(self, unit_id: str = None):
        """

        :param topic: Can be "financials", "confirmation", "check_in", "check_out"
        """
        self.account_sid = secrets['twilio']['account_sid']
        self.auth_token = secrets['twilio']['auth_token']

        self.client = Client(self.account_sid, self.auth_token)
        self.unit_id = unit_id

    def send_message(self, topic: str, **kwargs):

        if topic == "financials":
            body = f"Updated numbers:\nFrom: {kwargs['infos']['from_date']}\nTo: {kwargs['infos']['to_date']}\nUnit: {kwargs['infos']['unit_id']}\n\n"
            for k in kwargs["financials"].items():
                # kwargs should be a dict of financial kpis in this case.
                body += f"{k[1]['display_name']}: {k[1]['cleaning_function'](k[1]['value'])} {k[1]['units']}\n"

            self.client.messages.create(
                body=body,
                from_='+17816307516',
                to='+436601644192'
            )

        else:
            NotImplementedError("In Development...")

    def new_booking_sms(self, unit, name, from_date, to_date):
        body = f"New Booking: {name}\nFlat: {unit}\nCheck-In: {from_date} \nCheck-Out: {to_date}"
        self.client.messages.create(
            body=body,
            from_='+17816307516',
            to='+436601644192'
        )
