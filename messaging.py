from twilio.rest import Client


class Message:
    def __init__(self, secrets):
        self.secrets = secrets

        self.client = Client(self.secrets['twilio']['account_sid'], self.secrets['twilio']['auth_token'])

    def send_email(self, target_email: str, body: str):
        """
        This function allows you to send an email. Useful for Booking.com automation.
        """
        pass

    def send_whatsapp(self, target_phone: str, body: str, title: str = None):
        """
        Sends a custom Whatsapp message
        """
        response = self.client.messages.create(from_="whatsapp:+436703085269",
                                               to=target_phone,
                                               body=body)
        return response

    def send_sms(self, target_phone: str, body: str, title: str = None):
        """
        Sends a custom text message
        """
        response = self.client.messages.create(
            body=body,
            from_="+436703085269",
            to=target_phone
        )
        return response
