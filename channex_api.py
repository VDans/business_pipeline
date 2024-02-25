import json
import logging
import requests


class Channex:
    """For now only staging"""
    def __init__(self, secrets, url="https://staging.channex.io/api/v1"):
        self.secrets = secrets
        self.url = url
        self.logger = logging.getLogger()
        self.headers = {
            "user-api-key": self.secrets["channex"]["api_key"],
            "Content-Type": "application/json"
        }

    def get_properties_list(self):
        """GET list of all properties"""
        response = requests.request(method="GET",
                                    url=f"{self.url}/properties",
                                    headers=self.headers)
        self.logger.info(f"Response: {response}")
        return response

    def get_room_types_list(self, property_id=None):
        """GET list of all room types"""
        add_filter = f"?filter[property_id]={property_id}" if property_id else ""
        response = requests.request(method="GET",
                                    url=f"{self.url}/room_types" + add_filter,
                                    headers=self.headers)
        self.logger.info(f"Response: {response}")
        return response

    def get_rates_list(self, property_id=None):
        """GET list of all room types"""
        add_filter = f"?filter[property_id]={property_id}" if property_id else ""
        response = requests.request(method="GET",
                                    url=f"{self.url}/rate_plans" + add_filter,
                                    headers=self.headers)
        self.logger.info(f"Response: {response}")
        return response

    def update_availability_single(self, pid_channex, rid_channex, date, n_available):
        payload = json.dumps({
            "availability": n_available,
            "date": date.strftime("%Y-%m-%d"),
            "property_id": pid_channex,
            "room_type_id": rid_channex
        })
        response = requests.request(method="POST",
                                    url=f"{self.url}/availability",
                                    headers=self.headers,
                                    data=payload)

        return response

    def update_availability_range(self, av_list: list):
        payload = json.dumps({
            "values": av_list
        })
        # self.logger.info(f"Sending payload {payload} to POST /rates")
        response = requests.request(method="POST",
                                    url=f"{self.url}/availability",
                                    headers=self.headers,
                                    data=payload)

        return response

    def update_restrictions_range(self, r_list: list):
        payload = json.dumps({
            "values": r_list
        })
        # self.logger.info(f"Sending payload {payload} to POST /rates")
        response = requests.request(method="POST",
                                    url=f"{self.url}/restrictions",
                                    headers=self.headers,
                                    data=payload)

        return response

    def get_booking_revision(self, revision_id):
        """Get booking revision by ID. Made for webhook usage."""
        response = requests.request(method="GET",
                                    url=f"{self.url}/booking_revisions/{revision_id}",
                                    # bookings/:id also possible?
                                    headers=self.headers)
        self.logger.info(f"Response: {response}")
        return response.json()

    def acknowledge_booking_revision(self, revision_id):
        """Acknowledge booking revision by ID. Made for webhook usage."""
        response = requests.request(method="POST",
                                    url=f"{self.url}/booking_revisions/{revision_id}/ack",
                                    headers=self.headers)
        self.logger.info(f"Response: {response}")
        return response
