"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Broker Properties

        # Documentation for the Producer API is available in
        # https://docs.confluent.io/current/clients/python.html
        # The avro producer is just wrapper on top of the standard
        # confluent python producer. The supported configuration values are dictated
        # by the underlying librdkafka, written in C. The documentation of all available
        # properties is available in https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        self.broker_properties = {

            # Default Kafka configurations
            'bootstrap.servers': 'PLAINTEXT://localhost:9092',
            'client.id': 'simulation_producer',

            # Avro schema
            'schema.registry.url': 'http://localhost:8081',

            # Batching configurations
            'linger.ms': 0.5,  # Number of ms to wait to accumulate messages to send
            'batch.num.messages': 100,  # Number of messages to accumulate before sending
            'queue.buffering.max.kbytes': 2097151  # Set to 2GB - default is 16 GB
        }

        # Configure admin client
        self.admin_client = AdminClient(
            {'bootstrap.servers': self.broker_properties['bootstrap.servers']}
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            config=self.broker_properties
        )

    # Check if the topic already exists
    def topic_exists(self):

        logger.info(f"Checking if topic {self.topic_name} already exists")
        topic_metadata = self.admin_client.list_topics(timeout=5)

        if self.topic_name in topic_metadata.topics:
            logger.info(f"Topic {self.topic_name} exists")
            return True
        else:
            return False

        return False

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # If topic does not exist, create it
        if not self.topic_exists():

            # Create new topic object
            new_topic = NewTopic(
                self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas)

            # Create new topic
            self.admin_client.create_topics([new_topic], operation_timeout=10.0)

            # Confirm that topic has been created

            logger.info(f"Confirming topic {self.topic_name} creation")

            # Maybe this is not the best idea to wait here.
            # However, better than not having any errors
            # Any suggestions on how to handle this ?
            time.sleep(0.1)
            if not self.topic_exists():
                logger.error(f"Could not create topic {self.topic_name}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("Closing producer")
        self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
