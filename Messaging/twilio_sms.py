import pandas as pd
from twilio.rest import Client


class SmsEngine:
    def __init__(self, secrets, unit_id: str = None):
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

    def new_booking_sms(self, event, unit, name, from_date, to_date, phone):
        body = f"{event}: {name}\nFlat: {unit}\nCheck-In: {from_date.strftime('%Y-%m-%d')} \nCheck-Out: {to_date.strftime('%Y-%m-%d')} \nPhone: {phone}"
        self.client.messages.create(
            body=body,
            from_='+17816307516',
            to='+436601644192'
        )

    def cleaner_sms(self, event: str, unit_id: str, job_date: pd.Timestamp, cleaner_phone_number: str, next_guests_n_nights=None, next_guests_n_guests=None):

        # "name": "Lili",
        # "phone_number": "+436641453354"
        # "name": "Moni",
        # "phone_number": "+436602058630"

        if event == "reservation":
            out1 = "Veränderung"
            body = f"{out1} in {unit_id}\nDatum: {job_date.strftime('%Y-%m-%d')}\nAnzahl Gäste: {next_guests_n_guests}\nAnzahl Nächte: {next_guests_n_nights}"
        elif event == "modification of booking":
            out1 = "Veränderung"
            body = f"{out1} in {unit_id}\nDatum: {job_date.strftime('%Y-%m-%d')}\nAnzahl Gäste: {next_guests_n_guests}\nAnzahl Nächte: {next_guests_n_nights}"
        elif event == "cancellation":
            out1 = "Stornierung"
            body = f"{out1} in {unit_id}\nDatum: {job_date.strftime('%Y-%m-%d')}\nKeine Hilfe mehr nötig."
        else:
            body = None
            out1 = None
            ValueError("Event unknown")

        self.client.messages.create(
            body=body,
            from_='+17816307516',
            to=cleaner_phone_number
        )
