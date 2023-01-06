import pandas as pd
from datetime import datetime


class Cleaner:
    def __init__(self, cleaner_id):
        self.cleaner_id = cleaner_id

        self.name = None
        self.whatsapp_answers = None
        self.availability = None
        self.jobs = None

        self.load_data()

    def load_data(self):
        """
        Reads the individual cleaner table in DB and assigns to the attributes.
        """
        df = pd.DataFrame(
            {
                "name": "Klaudia",
                "whatsapp_answers": "Yes",
                "availability": datetime(day=15, month=11, year=2022),
                "jobs": datetime(day=15, month=11, year=2021)
            }
        )

        self.name = df["name"]

    def is_available(self, checked_date):
        return checked_date.isin(self.availability)

    def get_availability(self):
        """
        Read from DB...
        """
        self.availability = []

    def get_all_jobs(self):
        """
        Read from DB...
        """
        self.jobs = 'All Jobs'

    def get_all_whatsapp_answers(self):
        """
        Read from DB...
        """
        self.whatsapp_answers = 'All Whatsapp Messages'

    @staticmethod
    def get_total_paid_amount(self):
        """
        Read from DB...
        """
        out = 'Sum of the total paid amounts for this cleaner id'
        return out
