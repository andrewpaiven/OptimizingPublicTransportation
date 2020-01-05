"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Faust App instantiation
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Input Kafka Topic
topic = app.topic("com.cta.v1.postgres.stations", value_type=Station)

# Output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# Faust Table
table = app.Table(
    "stations_resumed",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


# Faust Agent that will process the topic transformation
@app.agent(topic)
async def process_stream(stations):
    async for station in stations:

        station_color = "red" if station.red else "blue" if station.blue else "green" if station.green else None

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station_color
        )
        table[station.station_id] = transformed_station

if __name__ == "__main__":
    app.main()
