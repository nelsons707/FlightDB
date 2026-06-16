import json
import time
from pathlib import Path

import requests
from confluent_kafka import Producer

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"
CREDS_PATH = Path(__file__).resolve().parent.parent / "Creds" / "OpenSkyCreds.json"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "opensky-states"

# Continental US bounding box (~1,450 sq deg -> 4 credits/call)
US_BBOX = {
    "lamin": 24.4,
    "lomin": -125.0,
    "lamax": 49.4,
    "lomax": -66.9,
}

POLL_INTERVAL_SECONDS = 300

# Column order returned by /states/all - mapped to field names for each message.
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


def load_credentials():
    with open(CREDS_PATH) as f:
        return json.load(f)


def get_access_token(client_id, client_secret):
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    resp.raise_for_status()
    token_data = resp.json()
    # expires_in is seconds; refresh a bit early to avoid using an expired token mid-poll.
    expires_at = time.time() + token_data["expires_in"] - 30
    return token_data["access_token"], expires_at


def fetch_states(access_token):
    resp = requests.get(
        STATES_URL,
        params=US_BBOX,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    resp.raise_for_status()
    return resp.json()


def to_messages(states_response):
    poll_time = states_response["time"]
    states = states_response["states"] or []
    messages = []
    for state in states:
        record = dict(zip(STATE_VECTOR_FIELDS, state))
        record["poll_time"] = poll_time
        messages.append(record)
    return messages


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed for {msg.key()}: {err}")


def publish(producer, messages):
    print(f"\n--- publishing {len(messages)} aircraft ---")
    for message in messages:
        producer.produce(
            KAFKA_TOPIC,
            key=message["icao24"],
            value=json.dumps(message),
            callback=delivery_report,
        )
    producer.flush()


def main():
    creds = load_credentials()
    access_token, expires_at = get_access_token(creds["clientId"], creds["clientSecret"])
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    while True:
        if time.time() >= expires_at:
            access_token, expires_at = get_access_token(creds["clientId"], creds["clientSecret"])

        states_response = fetch_states(access_token)
        messages = to_messages(states_response)
        publish(producer, messages)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
