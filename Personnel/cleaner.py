import logging
import pandas as pd


class Personnel:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def add_personnel(self, first_name, family_name, phone_number, role, ssn):
        """
        Add a row in the personnel table in the DB.
        """
        cleaner_id = first_name[:2] + "_" + family_name[:2]
        cleaner_row = pd.DataFrame([{
            "personnel_id": cleaner_id,
            "first_name": first_name,
            "family_name": family_name,
            "phone_number": phone_number,
            "role": role,
            "ssn": ssn
        }])
        cleaner_row.to_sql(name="personnel",
                           con=self.db_handler,
                           if_exists="append",
                           index=False)
        logging.info(f"New cleaner {first_name} added to the database.")

    def get_cleaner_schedule(self, cleaner_id):
        """
        Recover the schedule of a staff member.
        cleanings is the table with all schedules.
        """
        cleanings = pd.read_sql(sql=f"""SELECT * FROM cleanings WHERE personnel_id = {cleaner_id}""",
                                con=self.db_handler,
                                index_col=None)

        return cleanings

    def get_cleaner_info(self, cleaner_id):
        cleaner = pd.read_sql(sql=f"""SELECT * FROM personnel WHERE personnel_id = {cleaner_id}""",
                              con=self.db_handler,
                              index_col=None)

        return cleaner
