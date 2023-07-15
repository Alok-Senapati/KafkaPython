import time
import random

from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import string
import logging

# Setup Logging to show INFO level messages and Timestamp, level, module and line number
logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(levelname)s] - {%(module)s  %(lineno)d} - %(message)s')


BOOTSTRAP_SERVERS = 'localhost:9092'
DATA_PATH = 'data/data.json'
TOPIC_PREFIX = "kafka_stream"


def get_random_string(length):
    """
    Generate a random string of fixed length
    :param length: Length
    :return: Random string
    """
    letters = string.ascii_lowercase
    # noinspection PyUnusedLocal
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def create_topic(kafka_client, topic_name):
    """
    Create a topic in Kafka
    :param kafka_client: KafkaAdminClient
    :param topic_name: Name of the topic
    :return:
    """
    logging.info(f"Creating Topic {topic_name}......................")
    try:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        kafka_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        logging.error(f"Error while creating topic {topic_name} - {e}......................")
        raise e


def produce_kafka(bootstrap_server, topic_name):
    """
    Produce data to Kafka
    :param bootstrap_server: KAFKA Bootstrap Server
    :param topic_name: Name of the topic
    :return:
    """
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server)
        data = open(DATA_PATH, 'rb')
        for line in data:
            logging.info(f"Producing data to Kafka - {line}......................")
            producer.send(topic_name, line)
            time.sleep(random.random() * 2)
    except Exception as e:
        logging.error(f"Error while producing data to Kafka - {e}......................")
        raise e


def main():
    """
    Main function to Create a topic and produce data to Kafka
    :return:
    """
    topic_name = f"{TOPIC_PREFIX}_{get_random_string(5)}"
    kafka_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    create_topic(kafka_client, topic_name)
    produce_kafka(BOOTSTRAP_SERVERS, topic_name)


if __name__ == '__main__':
    main()
