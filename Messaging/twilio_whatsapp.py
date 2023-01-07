import json

from flask import Flask, request, redirect
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client

secrets = json.load(open('../secrets.json'))


class Whatsapp:
    def __init__(self):
        self.account_sid = secrets['twilio']['account_sid']
        self.auth_token = secrets['twilio']['auth_token']

        self.client = Client(self.account_sid, self.auth_token)

    def send_whatsapp_message(self, approved=False):
        if not approved:
            m_template = self.client.messages.create(
                from_='whatsapp:+17816307516',
                to='whatsapp:+436601644192',
                body=   """Bonjour !
                        Je suis Valentin, votre hôte pour Vienne.
                        Nous avons bien reçu votre réservation, et vous recevrez des informations sur l'enregistrement peu avant votre arrivée.
                        Veuillez utiliser ce numéro si vous avez des questions !
                        Bien à vous, 
                        Valentin
                        """
            )

        else:
            m_approved = self.client.messages.create(
                from_='whatsapp:+17816307516',
                media_url=["https://hips.hearstapps.com/hmg-prod.s3.amazonaws.com/images/how-to-keep-ducks-call-ducks-1615457181.jpg"],
                body="""""",
                to='whatsapp:+436601644192'
            )
