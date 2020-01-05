"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):

    # Load avro key schema
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    # Load avro value schema
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        self.station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(
            f"com.cta.v1.turnstile_events.{self.station_name}",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,  # Order of turnstile events is not important
            num_replicas=1  # Only 1 kafka broker
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        if num_entries != 0:
            logger.info(f"Creating {num_entries} turnstile messages for {self.station_name}")

            for num in range(num_entries):
                self.producer.produce(
                    topic=self.topic_name,
                    key_schema=self.key_schema,
                    value_schema=self.value_schema,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station_name,
                        "line": self.station.color.value
                    }
                )
