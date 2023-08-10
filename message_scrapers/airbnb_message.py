import logging
import time
import re
import datetime
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
options = webdriver.ChromeOptions()
options.add_argument("start-maximized")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# options.add_argument(r"user-data-dir=" + "C:/Users/Valentin/AppData/Local/Programs/Python/Python39/Lib/site-packages/selenium/profile/wpp")
options.add_argument('--profile-directory=Default')
options.add_argument('--user-data-dir=C:/Temp/ChromeProfile')


class AirbnbCom:
    """

    """
    def __init__(self, secrets, save_cookies = False):
        self.secrets = secrets
        self.save_cookies = save_cookies

        self.url = "https://www.airbnb.com/hosting"
        self.login_username = self.secrets['airbnb_scraper']['username']
        self.login_password = self.secrets['airbnb_scraper']['password']
        self.logger = logging.getLogger(__name__)
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

    def stop_scraper(self):
        self.logger.info("Stopping the WebDriver")
        self.driver.quit()

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
            continue_button = self.driver.find_element(By.CLASS_NAME, "l1j9v1wn")
            continue_button.click()

            time.sleep(1)

            pw = self.driver.find_element(By.ID, "email-signup-password")
            pw.send_keys(self.login_password)
            login_button = self.driver.find_elements(By.CLASS_NAME, "l1j9v1wn")[2]
            login_button.click()

        except NoSuchElementException:
            self.logger.info("Login Instructions not needed or not found. Moving on..")

    def send_message(self, thread_id,  message=None, image_path=None):
        """
        Attempt at sending a message automatically.
        Dreaming of eventually adding a picture in headless.
        """
        # Access the relevant thread:
        thread_url = f"https://www.airbnb.com/hosting/inbox/folder/all/thread/{thread_id}"
        self.driver.get(thread_url)
        time.sleep(2)
        self.logger.info("Accessing in stealth: " + thread_url)

        if message:
            pass

        if image_path:
            # The image upload input tag is hidden!

            wait = WebDriverWait(self.driver, 50)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".i9gn8a input[type=file]")))
            # elements = self.driver.find_elements(By.CSS_SELECTOR, ".file-upload-input input[type=file]")
            # .send_keys('C:\\Users\\Desktop\\test.png')
            print("")
