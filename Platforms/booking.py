import logging
import time
import json
import pickle
import datetime
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, InvalidCookieDomainException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium_stealth import stealth

options = webdriver.ChromeOptions()
options.add_argument("start-maximized")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument(r"user-data-dir=" + "C:/Users/Valentin/AppData/Local/Programs/Python/Python39/Lib/site-packages/selenium/profile/wpp")
options.add_argument('lang=en-US')
# options.add_argument("--no-sandbox")
# options.add_argument("--headless")
# options.add_argument("--disable-gpu")

secrets = json.load(open('../config_secrets.json'))


class BookingCom:
    """
    This class should allow the webscraping of new reservations on the platform Booking.com
    """

    def __init__(self,
                 unit_id,
                 url: str = "https://admin.booking.com/hotel/hoteladmin/groups/reservations/index.html?lang=en",
                 save_cookies: bool = False):

        self.unit_id = unit_id
        self.url = url

        self.save_cookies = save_cookies
        self.login_username = secrets['booking']['username']
        self.login_password = secrets['booking']['password']
        self.logger = logging.getLogger("booking_logger")
        self.driver = None

        self.initiate_driver()
        # self.go_to_page()
        # self.log_in()

    def initiate_driver(self):
        self.driver = webdriver.Chrome(executable_path="Resources/chromedriver.exe", options=options)
        self.driver.delete_all_cookies()

        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Booking.com",
                platform="Win64",
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
            # Load Cookies (Doesn't work rn...)
            cookies = pickle.load(open("Resources/cookies.pkl", "rb"))
            for cookie in cookies:
                self.driver.add_cookie(cookie)
        except InvalidCookieDomainException:
            self.logger.info("Cookies refused. Moving on...")

        try:

            username = self.driver.find_element(By.ID, "loginname")
            username.send_keys(self.login_username)
            continue_button = self.driver.find_element(By.XPATH,
                                                       '//*[@id="root"]/div/div/div/div[2]/div[1]/div/div/div/div/div/div/form/div[3]/button/span')
            continue_button.click()
            time.sleep(1)

            pw = self.driver.find_element(By.ID, "password")
            pw.send_keys(self.login_password)
            continue_button = self.driver.find_element(By.XPATH,
                                                       '//*[@id="root"]/div/div/div/div[2]/div[1]/div/div/div/div/div/div/form/div[2]/button/span')
            continue_button.click()

            if self.save_cookies:
                # If you want to save cookies, do it here:
                pickle.dump(self.driver.get_cookies(), open("Resources/cookies.pkl", "wb"))

        except NoSuchElementException:
            self.logger.info("Login Instructions not needed or not found. Moving on..")

        time.sleep(3)

    def stop_scraper(self):
        self.logger.info("Stopping the WebDriver")
        self.driver.quit()

    def get_bookings(self, from_date: pd.Timestamp, to_date: pd.Timestamp, unit_id):
        """
        Fetch the bookings on the admin portal.

        :param unit_id: Which apartment you want to get the bookings of.
        :param from_date: From which date do you want to fetch bookings?
        :param to_date: Until when?
        :return: a pandas Dataframe with all bookings of the apt you want in the dates you want within self.bookings.
        """
        self.url = f"https://admin.booking.com/hotel/hoteladmin/groups/reservations/index.html?lang=en&dateFrom={from_date.strftime('%Y-%m-%d')}&dateTo={to_date.strftime('%Y-%m-%d')}&ses=0c6fd8b00c60eba2679668efa5dd58d3"
        self.logger.info("Changing URL in stealth to: " + self.url)
        self.driver.get(self.url)

        self.log_in()

        self.logger.info("""I'm in!""")

        time.sleep(15)
        # WebDriverWait(self.driver, 30).until(lambda x: x.find_element(By.CLASS_NAME, "bui-table__cell"))

        bookings = []

        while True:
            # You are now seeing the reservations page. Pull the data for the bookings one by one:
            rows = self.driver.find_elements(By.CLASS_NAME, "bui-table__row")
            rows = rows[1:]  # The first row is the column names, not needed.
            bookings = self.quick_scrape(rows=rows, bookings=bookings)

            try:
                next_page_button = self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[2]/div/nav/div/div[3]/button')
            except NoSuchElementException:
                break

            if next_page_button.is_enabled():
                next_page_button.click()
                self.logger.info("Next page detected... Moving..")
                time.sleep(10)
            else:
                break

        bookings = [b for b in bookings if b["unit_id"] in unit_id]
        bookings = [b for b in bookings if b["check_in"] >= from_date]
        bookings = [b for b in bookings if b["check_out"] <= to_date]

        self.logger.info("Scraped summary information.")
        for booking in bookings:
            self.logger.info("Scraping Booking Guest: " + booking['guest_name'])
            booking = self.get_booking_details(booking)

        return pd.DataFrame(bookings)

    def get_booking_details(self, booking):
        """
        Scrape additional infos about a bookin directly on its url.
        url always has the same format: https://admin.booking.com/hotel/hoteladmin/extranet_ng/manage/booking.html?res_id={BOOKING_NUMBER}&hotel_id={UNIT_ID}

        The additional data needed includes the phone number, the email, the country of origin
        :return:
        """
        url = f"https://admin.booking.com/hotel/hoteladmin/extranet_ng/manage/booking.html?res_id={booking['booking_number']}&hotel_id={booking['unit_id']}ses=0c6fd8b00c60eba2679668efa5dd58d3"
        # Go to page
        self.driver.get(url)

        # Country of origin
        booking['country'] = self.driver.find_element(By.CLASS_NAME, "bui-flag__text").text.title()

        # Genius:
        # is it a genius client? Those have a picture next to their name ("")
        # is_genius = row.find_elements(By.TAG_NAME, "img")
        # booking['is_genius'] = False if len(is_genius) == 0 else True

        # Details on price:
        # You need to open the accordion first to expose the prices:
        a_o = self.driver.find_elements(By.CLASS_NAME, 'bui-accordion__row')
        a_o_price = [a for a in a_o if "€" in a.text][0]  # Safe?
        a_o_price.click()

        fees = self.driver.find_elements(By.CLASS_NAME, 'bui-table__row')
        n_fee = [f for f in fees if "Subtotal" in f.text]

        try:
            booking['nights_fee'] = round(float(n_fee[0].text.split()[-1]), 2)
            c_fee = [f for f in fees if "Cleaning" in f.text]
        except:
            booking['nights_fee'] = 0

        try:
            booking['cleaning_fee'] = round(float(c_fee[0].text.split()[-1]), 2)
        except:
            booking['cleaning_fee'] = 0

        booking['discount'] = 0
        booking['payment_commission'] = 0.012 * (booking['nights_fee'] + booking['cleaning_fee'])
        booking['platform_commission'] -= booking['payment_commission']

        # The following elements are only accessible if your booking is in the future, not in the past!
        # Phone Number: You will need to click a button, which then shows the number.
        phone_button = self.driver.find_elements(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div/div[2]/div/address/p[2]/div/div/span[2]/button')
        if len(phone_button) > 0:  # Older / Canceled booking don't have this anymore.
            phone_button[0].click()
            time.sleep(1)
            try:
                booking['phone_number'] = self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div/div[2]/div/address/p[2]/div/div/span[2]/a').text.replace(" ", "")
            except NoSuchElementException:
                time.sleep(2)
                booking['phone_number'] = self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div/div[2]/div/address/p[2]/div/div/span[2]/a').text.replace(" ", "")

        else:
            booking['phone_number'] = None

        # Email
        email = self.driver.find_elements(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div/div[2]/div/address/p[2]/a')
        if len(email) > 0:  # Older / Canceled booking don't have this anymore.
            booking['email'] = email[0].text
        else:
            booking['email'] = None

        return booking

    @staticmethod
    def quick_scrape(rows, bookings):
        for row in rows:
            booking = {}

            if row.text == '':  # The end of the list.
                break

            tcs = row.find_elements(By.CLASS_NAME, "bui-table__cell")
            tcs_texts = [tc.text for tc in tcs]

            # Collect the data:
            booking['unit_id'] = tcs_texts[0]
            booking['guest_name'] = tcs_texts[3].splitlines()[0].title()
            booking['n_guests'] = tcs_texts[3].splitlines()[1]
            booking['check_in'] = datetime.datetime.strptime(tcs_texts[4], '%b %d, %Y')
            booking['check_out'] = datetime.datetime.strptime(tcs_texts[5], '%b %d, %Y')
            booking['booking_date'] = datetime.datetime.strptime(tcs_texts[10], '%b %d, %Y')
            booking['status'] = tcs_texts[6].split()[0] if tcs_texts[6].split()[0] != 'Smart' else tcs_texts[6].split()[
                2]

            # Price received with cleaning fee, before commissions
            booking['platform_commission'] = round(float(tcs_texts[8].split()[1]), 2)

            booking['booking_number'] = tcs_texts[9]
            booking['platform'] = "Booking.com"

            # Add to the output:
            bookings.append(booking)

        return bookings
