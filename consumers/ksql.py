"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
   station_id INTEGER,
   station_name VARCHAR,
   line INTEGER
) WITH (
    KAFKA_TOPIC = 'com.cta.v1.turnstile_events',
    VALUE_FORMAT = 'Avro',
    KEY= 'station_id'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT = 'JSON') AS
    SELECT STATION_ID, COUNT(station_id) AS count FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""

    print("Creating KSQL table")

    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to Connector {json.dumps(resp.json(), indent=2)}")
        return

    print("KSQL Statement sent with success")


if __name__ == "__main__":
    execute_statement()
