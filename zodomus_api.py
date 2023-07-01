import logging
import time

import pandas as pd
import requests
import json
from requests.auth import HTTPBasicAuth


class Zodomus:
	def __init__(self, secrets, url="https://api.zodomus.com"):
		self.secrets = secrets
		self.url = url
		self.logger = logging.getLogger()
		self.headers = {
			# "Authorization": "Bearer " + self.secrets["zodomus_dev"][""],
			'Content-Type': 'application/json'
		}
		self.auth = HTTPBasicAuth(self.secrets["zodomus_prd"]["api_user"], self.secrets["zodomus_prd"]["api_password"])

	def custom_api_call(self, method, call_url, payload):
		"""Use when in need of additional custom API empty calls on the Zodomus server"""
		self.logger.info(f"Sending empty payload to {method} {call_url}")
		response = requests.request(auth=self.auth,
									method=method,
									url=f"{self.url}{call_url}",
									headers=self.headers,
									data=payload)
		return response

	def set_availability(self, channel_id: str, unit_id_z: str, room_id_z: str, date_from: pd.Timestamp, date_to: pd.Timestamp, availability: int):
		"""Change the number of available units of a property on both Airbnb and Booking.com"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"roomId": room_id_z,
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d"),
			"availability": availability
		})
		self.logger.info(f"Sending payload {payload} to POST /availability")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/availability",
									headers=self.headers,
									data=payload)
		return response

	def check_availability(self, unit_id_z: str, channel_id: str, date_from: pd.Timestamp, date_to: pd.Timestamp):
		"""Check the availability for a given unit and dates."""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d")
		})
		self.logger.info(f"Sending payload {payload} to GET /availability")
		response = requests.request(auth=self.auth,
									method="GET",
									url=f"{self.url}/availability",
									headers=self.headers,
									data=payload)
		return response

	def get_channels(self):
		"""Get available channels"""
		self.logger.info(f"Sending empty payload to GET /channels")
		response = requests.request(auth=self.auth,
									method="GET",
									url=f"{self.url}/channels",
									headers=self.headers)

		return response

	def activate_property(self, channel_id, unit_id_z, price_model="1"):
		"""Activate a new property on Zodomus"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"priceModelId": price_model
		})
		self.logger.info(f"Sending payload {payload} to POST /property-activation")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/property-activation",
									headers=self.headers,
									data=payload)

		return response

	def check_property(self, channel_id, unit_id_z):
		"""Check a new property on Zodomus"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z
		})
		self.logger.info(f"Sending payload {payload} to POST /property-check")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/property-check",
									headers=self.headers,
									data=payload)

		return response

	def activate_room(self, channel_id, unit_id_z, room_id_z, room_name_z: str = "One-Bedroom Apartment", n_rooms: int = 1):
		"""Activate one or more rooms for a property on a channel"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"rooms": [
				{
					"roomId": room_id_z,
					"roomName": room_name_z,
					"quantity": n_rooms,
					"status": 1,
					"rates": [
						f"{room_id_z}991"
					]
				}
			]
		})
		self.logger.info(f"Sending payload {payload} to POST /rooms-activation")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/rooms-activation",
									headers=self.headers,
									data=payload)

		return response

	def get_rooms_rates(self, channel_id, unit_id_z):
		"""Get rooms and rates for a given property"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z
		})
		self.logger.info(f"Sending payload {payload} to GET /room-rates")
		response = requests.request(auth=self.auth,
									method="GET",
									url=f"{self.url}/room-rates",
									headers=self.headers,
									data=payload)

		return response

	def set_rate(self, channel_id, unit_id_z, room_id_z: str, rate_id_z: str, date_from: pd.Timestamp, price: float, currency: str = "EUR"):
		"""
		Set a night price
		If the delta between dates is >1, then the same price will be applied to the range of dates."
		"""
		date_to = date_from + pd.Timedelta(days=1) if channel_id != '3' else date_from  # The function should be used for only one day at the time. .
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"roomId": room_id_z,
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d"),
			"currencyCode": currency,
			"rateId": rate_id_z,
			"prices": {
				"price": price
			}
		})
		self.logger.info(f"Sending payload {payload} to POST /rates")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/rates",
									headers=self.headers,
									data=payload)

		return response

	def set_airbnb_rate(self, channel_id, unit_id_z, room_id_z: str, rate_id_z: str, date_from: pd.Timestamp, price: float, min_nights: int, currency: str = "EUR"):
		"""
		Set a night price
		If the delta between dates is >1, then the same price will be applied to the range of dates."
		"""
		date_to = date_from + pd.Timedelta(days=1) if channel_id != '3' else date_from  # The function should be used for only one day at the time. .
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"roomId": room_id_z,
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d"),
			"currencyCode": currency,
			"rateId": rate_id_z,
			"prices": {
				"price": price
			},
			"minimumStay": min_nights
		})
		self.logger.info(f"Sending HEAVY payload {payload} to POST /rates")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/rates",
									headers=self.headers,
									data=payload)

		return response

	def set_minimum_nights(self, channel_id, unit_id_z, room_id_z: str, rate_id_z: str, date_from: pd.Timestamp, min_nights: int, currency: str = "EUR"):
		"""
		Set a minimum length of stay for a given date.
		"""
		date_to = date_from + pd.Timedelta(days=1) if channel_id != '3' else date_from  # The function should be used for only one day at the time. .
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"roomId": room_id_z,
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d"),
			"currencyCode": currency,
			"rateId": rate_id_z,
			"minimumStay": min_nights
		})
		self.logger.info(f"Sending payload {payload} to POST /rates")
		response = requests.request(auth=self.auth,
									method="POST",
									url=f"{self.url}/rates",
									headers=self.headers,
									data=payload)

		return response

	def set_rate_range(self, channel_id, unit_id_z, room_id_z: str, rate_id_z: str, date_price_min_nights: pd.DataFrame, currency: str = "EUR"):
		"""
		"set_rate()" for uploading different prices.
		Airbnb should not get too many calls at the same time, therefore includes a sleep function.
		"""
		for row in date_price_min_nights:
			time.sleep(0.1)
			d = row["date"]
			p = row["price"]
			m_n = row["min_nights"]
			self.set_rate(channel_id, unit_id_z, room_id_z, rate_id_z, d, p, currency)

	def get_reservations_summary(self, channel_id, unit_id_z):
		"""Overview of reservations"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z
		})
		self.logger.info(f"Sending payload {payload} to GET /reservations-summary")
		response = requests.request(auth=self.auth,
									method="GET",
									url=f"{self.url}/reservations-summary",
									headers=self.headers,
									data=payload)

		return response

	def get_reservations_queue(self, channel_id, unit_id_z):
		"""Get the queue of the newest changes in reservations"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z
		})
		self.logger.info(f"Sending payload {payload} to GET /reservations-queue")
		response = requests.request(auth=self.auth,
									method="GET",
									url=f"{self.url}/reservations-queue",
									headers=self.headers,
									data=payload)

		return response

	def get_reservation(self, channel_id: str, unit_id_z: str, reservation_number: str):
		"""Get details about a reservation"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"reservationId": reservation_number
		})
		self.logger.info(f"Sending payload {payload} to GET /reservations")
		response = requests.request(auth=self.auth,
									method="GET",
									url=f"{self.url}/reservations",
									headers=self.headers,
									data=payload)

		return response
