import json
import logging
import pandas as pd
from Platforms.booking import BookingCom
from Platforms.airbnb import AirbnbCom
from Messaging.twilio_sms import SmsEngine
from Messaging.twilio_whatsapp import Whatsapp
from Financials.financials import Financials
from sqlalchemy import types

secrets = json.load(open('../secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))


class Manager:
    """
    This class is the mother class coordinating all systems:
        NEW-CANCELLED-MODIFIED BOOKING:
        -> Enter the data into the database.
        -> Send text message to guest on a schedule
        -> Notify a cleaner of the new schedule/of the change.
    """
    def __init__(self,
                 db_connection,
                 from_date,
                 to_date,
                 unit_id=None):

        self.db_connection = db_connection
        self.from_date = from_date
        self.to_date = to_date
        self.unit_id = ["7332800", "9289798", "9347180", "9388506"] if not unit_id else unit_id

        self.latest_bookings = None
        self.db_bookings = None
        self.sms_er = SmsEngine(unit_id=self.unit_id)
        self.whatsapp_er = Whatsapp()

        self.logger = logging.getLogger("booking_logger")

    # Automated tasks:
    def get_latest_bookings(self, platform: list):
        """
        This automated task should update the database of bookings in the next month.
        :param platform: One of "booking.com", "airbnb", or "all"
        """

        if "airbnb" in platform:
            self.latest_bookings = self.scrape_airbnb()

        if "booking" in platform:
            if self.latest_bookings is not None:
                self.latest_bookings = pd.concat([self.latest_bookings, self.scrape_booking()], ignore_index=True)
            else:
                self.latest_bookings = self.scrape_booking()

    def get_db_bookings(self):
        """
        Get the bookings from the db
        """

        self.db_bookings = pd.read_sql(
            sql="select * from bookings where check_in >= %(from_date)s and check_in <= %(to_date)s and unit_id in %(unit_id)s",
            con=self.db_connection,
            params={
                "from_date": self.from_date.strftime('%Y-%m-%d'),
                "to_date": self.to_date.strftime('%Y-%m-%d'),
                "unit_id": tuple(self.unit_id)
            })

    def update_bookings(self, platform=None):
        if platform is None:
            platform = ["booking", "airbnb"]

        self.get_latest_bookings(platform=platform)  # Scrapes the latest bookings.
        self.get_db_bookings()  # Pull the db bookings.
        canceled_bookings = []

        if len(self.latest_bookings['booking_number']) > 0:
            for bn in self.latest_bookings['booking_number']:
                print("")
                if bn not in list(self.db_bookings['booking_number']):
                    # 1. NEW BOOKINGS
                    new_booking = self.latest_bookings[self.latest_bookings['booking_number'] == bn]
                    self.logger.info(new_booking['guest_name'] + " has been added to the database.")
                    new_booking.to_sql(name='bookings',
                                       con=self.db_connection,
                                       if_exists="append",
                                       index=None,
                                       dtype={
                                           "commission": types.FLOAT
                                       })

                else:
                    # 2. BOOKING MODIFICATIONS:
                    latest_status = self.latest_bookings['status'].loc[self.latest_bookings['booking_number'] == bn].values[0]
                    db_status = self.db_bookings['status'].loc[self.db_bookings['booking_number'] == bn].values[0]
                    guest_name = self.db_bookings['guest_name'].loc[self.db_bookings['booking_number'] == bn].values[0]

                    if latest_status != db_status:
                        canceled_bookings.append(bn)
                        self.logger.info(f"{guest_name}'s status was changed to 'Canceled'.")
                        sql = "update bookings set status = 'Canceled' where booking_number = %(bn)s"
                        self.db_connection.execute(sql, {'bn': bn})

                    else:
                        self.logger.info(f"No change detected for {guest_name}.")

    def get_financials(self, send_sms: bool = False):
        f = Financials(unit_id=self.unit_id,
                       db_connection=self.db_connection)
        f.compute_kpis(from_date=self.from_date, to_date=self.to_date)

        if send_sms:
            self.sms_er.send_message(topic="financials",

                                     financials=f.kpis,
                                     infos={
                                         "from_date": self.from_date.strftime('%Y-%m-%d'),
                                         "to_date": self.to_date.strftime('%Y-%m-%d'),
                                         "unit_id": self.unit_id
                                     })

    def prepare_cleaning_plan(self):
        """
        Prepare the cleaning table for the cleaners. Only working from January, when the units are separate.
        Should display all dates between from_date and to_date, and add the corresponding next number of guests.
        :return: An Excel file with a table indicating the apartment, the date and the name and number of guests to clean for.
        """

        self.get_db_bookings()
        inputs = self.db_bookings[self.db_bookings['status'] != 'Canceled']  # Take out canceled events.
        all_dates_between = list(pd.date_range(start=self.from_date, end=self.to_date))

        inputs["check_out"] = [adb.strftime("%Y-%m-%d") for adb in inputs["check_out"]]
        all_dates_between = [adb.strftime("%Y-%m-%d") for adb in all_dates_between]

        out = pd.DataFrame({"check_out": all_dates_between})

        for uid in self.unit_id:
            coln = resources["display_names"][uid]
            unit_inputs = inputs[inputs['unit_id'] == uid]
            unit_inputs[coln] = unit_inputs.n_guests.shift(-1)
            unit_inputs = unit_inputs[["check_out", coln]]
            out = out.merge(unit_inputs, on='check_out', how='left')

        out = out.fillna('0')

        return out

    def text_val(self, approved):
        self.whatsapp_er.send_whatsapp_message(approved=approved)

    def scrape_airbnb(self):
        # Airbnb
        ab = AirbnbCom(unit_id=self.unit_id)
        out = ab.get_bookings(from_date=self.from_date,
                              to_date=self.to_date,
                              unit_id=self.unit_id)
        return out

    def scrape_booking(self):
        # Booking.com
        bc = BookingCom(unit_id=self.unit_id)
        out = bc.get_bookings(from_date=self.from_date,
                              to_date=self.to_date,
                              unit_id=self.unit_id)
        bc.stop_scraper()
        return out
