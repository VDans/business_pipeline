class Message:
    def __init__(self):
        pass

    def send_email(self, target_email: str, body: str):
        """
        This function allows you to send an email. Useful for Booking.com automation.
        """
        pass

    def send_whatsapp(self, target_phone: str, body: str, title: str = None):
        """
        Sends a custom Whatsapp message
        """
        pass

    def send_sms(self, target_phone: str, body: str, title: str = None):
        """
        Sends a custom text message
        """
        pass
