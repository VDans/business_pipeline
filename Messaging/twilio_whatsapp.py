import pandas as pd
from twilio.rest import Client


class Whatsapp:
    def __init__(self, secrets, resources):
        self.secrets = secrets
        self.resources = resources
        self.account_sid = self.secrets['twilio']['account_sid']
        self.auth_token = self.secrets['twilio']['auth_token']

        self.client = Client(self.account_sid, self.auth_token)

    def send_whatsapp_message(self):
        m_template = self.client.messages.create(
            from_=self.resources["twilio"]["whatsapp_sender"],
            to="whatsapp:+436601644192",
            body="""Hello!

                    I hope you are doing well!
                    Hereby some help for the way to your apartment.
                    
                    Please come to Erdbergstrasse 118, 1030 Vienna. The closest station is Kardinal-Nagl Platz, 50 meters away.
                    Please ring at number 32, with Kasbacher written on it. After 3 seconds, you will be let in automatically.
                    Please go to the 3rd floor, and then left, until you find a large white door with the number 32.
                    There, on the door you will see a code pad. Enter the 6 digits code, and the door will open for you.
                    
                    Once in the apartment, a physical key will be waiting for you on the kitchen counter.
                    
                    Have a very nice trip, and enjoy Vienna!
                    
                    Kind regards, 
                    Valentin""")
        """
        HOW TO SEND A PICTURE
        else:
            m_approved = self.client.messages.create(
                from_='whatsapp:+17816307516',
                media_url=["https://hips.hearstapps.com/hmg-prod.s3.amazonaws.com/images/how-to-keep-ducks-call-ducks-1615457181.jpg"],
                body="""""",
                to='whatsapp:+436601644192'
            )
        """
        return m_template

    def text_cleaner(self, event: str, unit_id: str, job_date: pd.Timestamp, cleaner_phone_number: str, next_guests_n_nights=None, next_guests_n_guests=None):
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

        m_template = self.client.messages.create(
            from_=self.resources["twilio"]["whatsapp_sender"],
            to="whatsapp:+436601644192",
            body="YOUR MESSAGE HERE")
