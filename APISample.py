import time
from pprint import pprint

import requests

BASE_URL = "https://opensky-network.org/api"

# Continental US bounding box
US_BBOX = {
    "lamin": 24.4,
    "lomin": -125.0,
    "lamax": 49.4,
    "lomax": -66.9,
}

# Column names for the "states" array returned by /states/all.
# OpenSky returns each state vector as a plain array (no field names),
# so we map the indices ourselves for readability.
STATE_VECTOR_FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]


def show_states_all():
    print("\n=== /states/all (US) ===")
    resp = requests.get(f"{BASE_URL}/states/all", params=US_BBOX)
    resp.raise_for_status()
    data = resp.json()

    print(f"time: {data['time']}")
    print(f"total aircraft: {len(data['states'])}")
    print("sample state vectors (raw arrays):")
    pprint(data["states"][:3])

    print("\nsample state vectors (mapped to field names):")
    for state in data["states"][:3]:
        pprint(dict(zip(STATE_VECTOR_FIELDS, state)))


def show_flights_all():
    print("\n=== /flights/all ===")
    # OpenSky limits this endpoint to a 2-hour window.
    end = int(time.time())
    begin = end - 2 * 60 * 60

    resp = requests.get(f"{BASE_URL}/flights/all", params={"begin": begin, "end": end})
    resp.raise_for_status()
    data = resp.json()

    print(f"total flights in last 2 hours: {len(data)}")
    print("sample flights:")
    pprint(data[:3])


def show_flights_by_aircraft():
    print("\n=== /flights/aircraft ===")
    # Pick an icao24 from the current states so this endpoint returns something.
    resp = requests.get(f"{BASE_URL}/states/all", params=US_BBOX)
    resp.raise_for_status()
    states = resp.json()["states"]
    icao24 = next(s[0] for s in states if s[0])

    end = int(time.time())
    begin = end - 24 * 60 * 60  # max 30-day window allowed

    resp = requests.get(
        f"{BASE_URL}/flights/aircraft",
        params={"icao24": icao24, "begin": begin, "end": end},
    )
    print(f"icao24: {icao24}")
    print(f"status: {resp.status_code}")
    if resp.ok:
        pprint(resp.json())
    else:
        print(resp.text)


if __name__ == "__main__":
    show_states_all()
    # show_flights_all()
    # show_flights_by_aircraft()
