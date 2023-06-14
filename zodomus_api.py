import logging
import pandas as pd
import requests
import json


class Zodomus:
	def __init__(self, url="https://api.zodomus.com"):
		self.url = url
		self.logger = logging.getLogger()
		self.headers = {
			'Content-Type': 'application/json',
			'Authorization': 'Basic bEVlNnNhUUkvR1A4RnZjRlIwTjNwejVEdG9YTlJCMEhJT3NldlFLcXduUT06VkVXY2FUMzY5ZW41ZEFyMUhrTEZvTFE2bGQvYVJ1NXF2aXRZdVZtRnVNdz0='
		}

	def set_availability(self, unit_id_z: str, date_from: pd.Timestamp, date_to: pd.Timestamp, availability: int):
		"""Change the number of available units of a property on both Airbnb and Booking.com"""
		booking_payload = json.dumps({
			"channelId": "1",
			"propertyId": unit_id_z,
			"roomId": f"{unit_id_z}01",
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d"),
			"availability": availability
		})
		airbnb_payload = json.dumps({
			"channelId": "3",
			"propertyId": unit_id_z,
			"roomId": f"{unit_id_z}01",
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d"),
			"availability": availability
		})
		self.logger.info(f"Sending payload {airbnb_payload} to POST /availability")
		response0 = requests.request(method="POST",
		                             url=f"{self.url}/availability",
		                             headers=self.headers,
		                             data=airbnb_payload)
		self.logger.info(f"Sending payload {booking_payload} to POST /availability")
		response1 = requests.request(method="POST",
		                             url=f"{self.url}/availability",
		                             headers=self.headers,
		                             data=booking_payload)
		return response0, response1

	def check_availability(self, unit_id_z: str, channel_id: str, date_from: pd.Timestamp, date_to: pd.Timestamp):
		"""Check the availability for a given unit and dates."""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z,
			"dateFrom": date_from.strftime("%Y-%m-%d"),
			"dateTo": date_to.strftime("%Y-%m-%d")
		})
		self.logger.info(f"Sending payload {payload} to GET /availability")
		response = requests.request(method="GET",
		                            url=f"{self.url}/availability",
		                            headers=self.headers,
		                            data=payload)
		return response

	def get_channels(self):
		"""Get available channels"""
		self.logger.info(f"Sending empty payload to GET /channels")
		response = requests.request(method="GET",
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
		response = requests.request(method="POST",
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
		response = requests.request(method="POST",
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
		response = requests.request(method="POST",
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
		response = requests.request(method="GET",
		                            url=f"{self.url}/room-rates",
		                            headers=self.headers,
		                            data=payload)

		return response

	def set_rate(self, channel_id, unit_id_z, room_id_z: str, rate_id_z: str, date_from: pd.Timestamp, date_to: pd.Timestamp, price: float, currency: str = "EUR"):
		"""Set a night price"""
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
		response = requests.request(method="POST",
		                            url=f"{self.url}/rates",
		                            headers=self.headers,
		                            data=payload)

		return response

	def get_reservations_summary(self, channel_id, unit_id_z):
		"""Overview of reservations"""
		payload = json.dumps({
			"channelId": channel_id,
			"propertyId": unit_id_z
		})
		self.logger.info(f"Sending payload {payload} to GET /reservations-summary")
		response = requests.request(method="GET",
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
		response = requests.request(method="GET",
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
		response = requests.request(method="GET",
		                            url=f"{self.url}/reservations",
		                            headers=self.headers,
		                            data=payload)

		return response
