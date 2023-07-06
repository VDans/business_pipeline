import json
import logging
import random

from messaging import Message


secrets = json.load(open('config_secrets.json'))

logging.basicConfig(level=logging.INFO)


def send_random_love_message():
    """
    Step 1: Pull a random love picture
    Step 2: Determine a random time between 10AM and 15AM
    Step 3: Send the whatsapp message
    """

    m = Message(secrets=secrets)
    n_repeat = random.randint(0, 4)
    emojis = ["❤️", "🥰", "🧸❤️", "💖", "👩‍❤️‍👨", ""]
    hearts_drawing = random.randint(1, 5)
    body = n_repeat * emojis[hearts_drawing]

    m.send_sms(target_phone="+436643964372", body=body)
    logging.info("Sent love sms successfully")


def main():
    send_random_love_message()


if __name__ == '__main__':
    main()
