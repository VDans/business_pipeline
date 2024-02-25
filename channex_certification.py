import json
import random
import logging
import pandas as pd
import datetime
from channex_api import Channex

secrets = json.load(open('config_secrets.json'))
c = Channex(secrets=secrets)
logging.getLogger().setLevel(logging.INFO)


# Functions defined for example usage.
def add_availability_snippet(av, av_list):
    availability_snippet = {
        "availability": av["availability"],
        "date_from": av["price_date"].strftime("%Y-%m-%d"),
        "date_to": (av["price_date"] + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        "property_id": av["channex_pid"],
        "room_type_id": av["channex_rid"]
    }

    av_list.append(availability_snippet)


def add_restrictions_snippet(av, r_list):
    restrictions_snippet = {
        "date_from": av["price_date"].strftime("%Y-%m-%d"),
        "date_to": (av["price_date"] + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        "property_id": av["channex_pid"],
        "room_type_id": av["channex_rid"],
        "rate_plan_id": av["channex_rtid"],
        "min_stay_through": av["min_nights"],
        "rate": av["price"]
    }
    r_list.append(restrictions_snippet)


def get_db_prices():
    """
    Normally this returns availabilities and prices from our internal DB.
    For the sake of certification, this returns dummy values
    """
    next_days = [pd.Timestamp.now() + datetime.timedelta(days=x) for x in range(500)]
    price_date = [pd.to_datetime(d).date() for d in next_days]
    availabilities = [round(random.random()) for _ in range(500)]
    prices = [int(100 * (random.random() + 1)) for _ in range(500)]
    min_nights = [int(10 * random.random()) + 1 for _ in range(500)]
    out = pd.DataFrame({"price_date": price_date, "channex_pid": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
                        "channex_rid": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
                        "channex_rtid": "1837d2ba-f232-4c8a-8843-e84689a90d4e", "availability": availabilities,
                        "price": prices, "min_nights": min_nights})

    return out


# Fetch Properties list:
response = c.get_properties_list().json()
test_property_id = response["data"][0]["attributes"]["id"]

# Fetch all Room Types Data
response = c.get_room_types_list().json()
all_room_type_id_list = [rt["attributes"]["id"] for rt in response["data"]]
# Get all bound with our test property:
response = c.get_room_types_list(property_id=test_property_id).json()
room_type_id_list = [rt["attributes"]["id"] for rt in response["data"]]

# Fetch all Rates Data
response = c.get_rates_list().json()
all_rates_list = [rt["attributes"]["id"] for rt in response["data"]]
# Get all bound with our test property:
response = c.get_room_types_list(property_id=test_property_id).json()
rates_list = [rt["attributes"]["id"] for rt in response["data"]]

# Insert mapping picture here.

# Test Cases
# 1. Full Data Update (Full Sync)
# In production, availabilities, min. nights & prices will be pulled as a pandas df from our internal DB.
# Here we use random numbers to simulate
# Availabilities
av_list = []
r_list = []
db_prices_df = get_db_prices()
db_prices_df.apply(add_availability_snippet, axis=1, args=(av_list,))
response = c.update_availability_range(av_list=av_list).json()
ids_response = [r["id"] for r in response["data"]]
# Prices
db_prices_df.apply(add_restrictions_snippet, axis=1, args=(r_list,))
response = c.update_availability_range(av_list=r_list).json()
ids_response = [r["id"] for r in response["data"]]

# 2. Single Date Update for Single Rate
# All property_ids, room types and rate plans will be dynamic, just hard coding it for the sake of certification.

input_list = [{
    "date": "2024-11-22",
    "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
    "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
    "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
    "rate": 333
}]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 3. Single Date Update for Multiple Rates
input_list = [
    {
        "date": "2024-11-21",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
        "rate": 333
    },
    {
        "date": "2024-11-25",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "bbdf5569-4c62-475a-82f7-a4ea089c4809",
        "rate": 444
    },
    {
        "date": "2024-11-29",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "1837d2ba-f232-4c8a-8843-e84689a90d4e",
        "rate": 456.23
    }
]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 4. Multiple Date Update for Multiple Rates
input_list = [
    {
        "date_from": "2024-11-01",
        "date_to": "2024-11-11",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
        "rate": 241
    },
    {
        "date_from": "2024-11-10",
        "date_to": "2024-11-16",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "bbdf5569-4c62-475a-82f7-a4ea089c4809",
        "rate": 312.66
    },
    {
        "date_from": "2024-11-01",
        "date_to": "2024-11-20",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "1837d2ba-f232-4c8a-8843-e84689a90d4e",
        "rate": 111
    }
]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 5. Min Stay Update
input_list = [
    {
        "date": "2024-11-21",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
        "min_stay_through": 3
    },
    {
        "date": "2024-11-25",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "bbdf5569-4c62-475a-82f7-a4ea089c4809",
        "min_stay_through": 2
    },
    {
        "date": "2024-11-29",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "1837d2ba-f232-4c8a-8843-e84689a90d4e",
        "min_stay": 5
    }
]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 6. Stop Sell Update
input_list = [
    {
        "date": "2024-11-21",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
        "stop_sell": True
    },
    {
        "date": "2024-11-25",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "bbdf5569-4c62-475a-82f7-a4ea089c4809",
        "stop_sell": True
    },
    {
        "date": "2024-11-29",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "1837d2ba-f232-4c8a-8843-e84689a90d4e",
        "stop_sell": True
    }
]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 7. Multiple Restrictions Update
input_list = [
    {
        "date_from": "2024-11-01",
        "date_to": "2024-11-10",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
        "closed_to_arrival": True,
        "closed_to_departure": False,
        "max_stay": 4,
        "min_stay": 1
    },
    {
        "date_from": "2024-11-12",
        "date_to": "2024-11-16",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "11d1b33c-eb12-4a29-8c8a-98ad0bacf3cf",
        "closed_to_arrival": False,
        "closed_to_departure": True,
        "min_stay": 6
    },
    {
        "date_from": "2024-11-10",
        "date_to": "2024-11-16",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "bbdf5569-4c62-475a-82f7-a4ea089c4809",
        "closed_to_arrival": True,
        "min_stay": 2
    },
    {
        "date_from": "2024-11-01",
        "date_to": "2024-11-20",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "1837d2ba-f232-4c8a-8843-e84689a90d4e",
        "min_stay": 10
    }
]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 8. Half-year Update
input_list = [
    {
        "date_from": "2024-12-01",
        "date_to": "2025-05-01",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
        "rate_plan_id": "d561d13f-97d9-4e64-bcba-afcd1e641452",
        "rate": 432,
        "closed_to_arrival": False,
        "closed_to_departure": False,
        "min_stay": 2
    },
    {
        "date_from": "2024-12-01",
        "date_to": "2025-05-01",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
        "rate_plan_id": "bbdf5569-4c62-475a-82f7-a4ea089c4809",
        "closed_to_arrival": True,
        "rate": 342,
        "min_stay": 3
}
]
response = c.update_restrictions_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 9. Single Date Availability Update
input_list = [
    {
        "availability": 7,  # Not possible as max availability has been set to 1 in this case. But I get it.
        "date": "2024-11-21",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
    },
    {
        "availability": 0,
        "date": "2024-11-25",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
    }
]
response = c.update_availability_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]

# 10. Multiple Date Availability Update
input_list = [
    {
        "availability": 3,  # Not possible as max availability has been set to 1 in this case. But I get it.
        "date_from": "2024-11-10",
        "date_to": "2024-11-16",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "49589fb9-0a10-42ef-9ad9-9d3a4981a578",
    },
    {
        "availability": 4,  # Not possible as max availability has been set to 1 in this case. But I get it.
        "date_from": "2024-11-17",
        "date_to": "2024-11-24",
        "property_id": "96387fc7-ba04-4bdd-b487-8d71411f0ce9",
        "room_type_id": "2af51941-2f22-40a4-a280-c9f8e0cee0cb",
    }
]
response = c.update_availability_range(input_list).json()
ids_response = [r["id"] for r in response["data"]]
