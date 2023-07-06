import json
import logging

import pandas as pd

from messaging import Message
from sqlalchemy import create_engine
from database_handling import DatabaseHandler


secrets = json.load(open('config_secrets.json'))

logging.basicConfig(level=logging.INFO)

db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)


def check_in_instructions():
    """
    Con
    """
    # 1. Get Bookings within 3 days:

    # 2. Run Checks:


def main():
    check_in_instructions()


if __name__ == '__main__':
    main()
