"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    logger.info("Creating stations Postgres connector")

    # Make HTTP Request
    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://postgres:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "com.cta.v1.postgres.",
               "poll.interval.ms": "60000",
               # Message should have a key - easier for processing after with faust
               "transforms": "ValueToKey",
               "transforms.ValueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
               "transforms.ValueToKey.fields": "station_id"
           }
       }),
    )

    try:
        resp.raise_for_status()
    except:
        logger.error(f"Failed to send data to Connector {json.dumps(resp.json(), indent=2)}")
        return

    logger.info("Postgres connector created successfully")


if __name__ == "__main__":
    configure_connector()
