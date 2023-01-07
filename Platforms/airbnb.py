import logging
import time
import json
import datetime
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium_stealth import stealth

options = webdriver.ChromeOptions()
options.add_argument("start-maximized")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument(
    r"user-data-dir=" + "C:/Users/Valentin/AppData/Local/Programs/Python/Python39/Lib/site-packages/selenium/profile/wpp")
options.add_argument('lang=en-US')
# options.add_argument("--no-sandbox")
# options.add_argument("--headless")
# options.add_argument("--disable-gpu")

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))


class AirbnbCom:
    """
    This class should allow the webscraping of new reservations on the platform Airbnb
    """

    def __init__(self,
                 unit_id,
                 url: str = "https://www.airbnb.com/hosting/reservations/all",
                 save_cookies: bool = False):

        self.unit_id = unit_id
        self.url = url
        self.save_cookies = save_cookies
        self.login_username = secrets['airbnb']['username']
        self.login_password = secrets['airbnb']['password']
        self.logger = logging.getLogger("booking_logger")
        self.driver = None

        self.initiate_driver()
        self.go_to_page()
        self.log_in()

    def initiate_driver(self):
        self.driver = webdriver.Chrome(executable_path="Resources/chromedriver.exe", options=options)
        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Booking.com",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                )

    def go_to_page(self):
        self.logger.info("Accessing in stealth: " + self.url)
        self.driver.get(self.url)

    def log_in(self):
        """
        Enter the username and password into the boxes, if asked.
        """
        try:
            email_login_choice = self.driver.find_element(By.XPATH, '//*[@id="FMP-target"]/div/div/div/div[3]/div/div[4]/button')
            email_login_choice.click()

            un = self.driver.find_element(By.ID, "email-login-email")
            un.send_keys(self.login_username)
            continue_button = self.driver.find_element(By.XPATH, '//*[@id="FMP-target"]/div/div/div/form/div[3]/button')
            continue_button.click()

            time.sleep(1)

            pw = self.driver.find_element(By.ID, "email-signup-password")
            pw.send_keys(self.login_password)
            login_button = self.driver.find_element(By.XPATH, '//*[@id="FMP-target"]/div/div/div/form/div/div[4]/button')
            login_button.click()

        except NoSuchElementException:
            self.logger.info("Login Instructions not needed or not found. Moving on..")

        time.sleep(1)

    def stop_scraper(self):
        self.logger.info("Stopping the WebDriver")
        self.driver.quit()

    def get_bookings(self, from_date: pd.Timestamp, to_date: pd.Timestamp, unit_id):
        """
        Fetch the bookings on the airbnb reservations page.

        Don't forget to filter by unit and by date from the inputs!

        :param unit_id: Which apartment you want to get the bookings of.
        :param from_date: From which date do you want to fetch bookings?
        :param to_date: Until when?
        :return: a pandas Dataframe with all bookings of the apt you want in the dates you want within self.bookings.
        """
        self.logger.info("""I'm in!""")

        bookings = []

        # There may be several pages to scrape. See bottom pagination numbers:
        selected_page = 1
        other_pages = self.driver.find_elements(By.CLASS_NAME, "_833p2h")
        other_pages_int = [int(o.text) for o in other_pages]
        max_page = max(other_pages_int)

        while True:
            # You are now seeing the reservations page. Pull the data for the bookings one by one:
            rows = self.driver.find_element(By.CSS_SELECTOR, 'table._iqk9th')
            rows = rows.find_elements(By.CSS_SELECTOR, 'tr')
            rows = rows[1:]  # The first row is the column names, not needed.
            bookings = self.quick_scrape(rows=rows, bookings=bookings)

            selected_page += 1
            if selected_page == max_page:
                break
            else:
                other_pages[selected_page - 2].click()
                time.sleep(3)

        bookings = [b for b in bookings if b["unit_id"] == unit_id]
        bookings = [b for b in bookings if b["check_in"] >= from_date]
        bookings = [b for b in bookings if b["check_out"] <= to_date]

        self.logger.info("Scraped summary information.")
        for booking in bookings:
            self.logger.info(f"Scraping details for {booking['guest_name']}")
            booking = self.get_booking_details(booking)

        out = pd.DataFrame(bookings)

        return out

    def get_booking_details(self, booking):
        """
        Scrape additional infos about a bookin directly on its url.

        The additional data needed includes the phone number, the email, the country of origin
        :return:
        """
        url = f"https://www.airbnb.com/hosting/reservations/upcoming?confirmationCode={booking['booking_number']}"
        # Go to page
        self.driver.get(url)

        time.sleep(5)

        # Country of origin:
        countries = self.driver.find_elements(By.CLASS_NAME, '_icmmqy')
        countries = [c for c in countries if "Lives in" in c.text]
        booking['country'] = countries[0].text.replace("Lives in ", "") if len(countries) > 0 else None

        # Phone Number:
        pn = self.driver.find_elements(By.CLASS_NAME, '_1pn58uu')
        booking['phone_number'] = pn[0].text.replace("Phone: ", "").replace("-", "").replace(" ", "") if len(pn) > 0 else None
        booking['phone_number'] = None if booking['phone_number'] == "Phonenumberunavailable" else booking['phone_number']

        # The next list represents all the spans with important values in them.
        # 1. Filter for the currency sign:
        vals = self.driver.find_elements(By.CLASS_NAME, '_157khfu')
        vals = [v for v in vals if "€" in v.text]

        # 2. Sometimes, a night adjustment comes in and makes the list length 9 instead of 8. Beware!
        # We are here interested in the host payout section:
        if len(vals) == 8:
            booking['nights_fee'] = round(float(vals[4].text.splitlines()[1].split()[1]), 2)
            booking['cleaning_fee'] = round(float(vals[5].text.splitlines()[1].split()[1]), 2)
            booking['discount'] = 0
            booking['platform_commission'] = round(float(vals[6].text.splitlines()[1].split()[1]), 2)
            booking['payment_commission'] = 0

        elif len(vals) == 9:
            booking['nights_fee'] = round(float(vals[4].text.splitlines()[1].split()[1]), 2)
            booking['cleaning_fee'] = round(float(vals[5].text.splitlines()[1].split()[1]), 2)
            booking['discount'] = round(float(vals[6].text.splitlines()[1].split()[1]), 2)
            booking['platform_commission'] = round(float(vals[7].text.splitlines()[1].split()[1]), 2)
            booking['payment_commission'] = 0

        else:
            ValueError("The length of the prices list is neither 8 nor 9.")

        # Email
        try:
            emails = self.driver.find_elements(By.CLASS_NAME, 'plmw1e5')
            n_email = [e for e in emails if "@" in e.text]
            email = n_email[0].text.split()[-1]
            booking['email'] = email
        except IndexError:
            booking['email'] = None

        return booking

    @staticmethod
    def quick_scrape(rows, bookings):
        for row in rows:
            booking = {}

            if row.text == '':  # The end of the list.
                break

            cells = row.find_elements(By.TAG_NAME, "td")

            booking['is_genius'] = False

            # Cell 0 has the status: Booked, Canceled, or Completed:
            if "Canceled" in cells[0].text:
                booking['status'] = "Canceled"
            elif "Past" in cells[0].text:
                booking['status'] = "Completed"
            elif "Review" in cells[0].text:
                booking['status'] = "Completed"
            else:
                booking['status'] = "Booked"

            # Cell 1 contains Name, and n_guests sperated by \n
            booking['guest_name'] = cells[1].text.splitlines()[0]
            booking['n_guests'] = cells[1].text.splitlines()[1]

            # Cell 2 is phone number, but hidden. To be gotten in detail later.
            # Cell 3, 4, 5 are check-in/out/booking date
            booking['check_in'] = datetime.datetime.strptime(cells[3].text, '%b %d, %Y')
            booking['check_out'] = datetime.datetime.strptime(cells[4].text, '%b %d, %Y')
            try:
                booking['booking_date'] = datetime.datetime.strptime(cells[5].text.splitlines()[0], '%b %d, %Y')
            except ValueError:
                booking['booking_date'] = datetime.datetime.now()

            booking['unit_id'] = resources['display_names'][cells[6].text]

            booking['payment_commission'] = 0

            booking['booking_number'] = cells[7].text

            booking['platform'] = "Airbnb"

            # Add to the output:
            bookings.append(booking)

        return bookings
