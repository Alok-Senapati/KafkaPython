import time
import random
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import logging
import sys

# Setup Logging to show INFO level messages and Timestamp, level, module and line number
logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(levelname)s] - {%(module)s  %(lineno)d} - %(message)s')


BOOTSTRAP_SERVERS = 'localhost:9092'
DATA_PATH = 'data/data.json'


def is_topic_exists(kafka_client, topic_name):
    """
    Check if a topic exists in Kafka
    :param kafka_client: KafkaAdminClient
    :param topic_name: Name of the topic
    :return:
    """
    logging.info(f"Checking if {topic_name} exists in Kafka......................")
    try:
        return topic_name in kafka_client.list_topics()
    except Exception as e:
        logging.error(f"Error while listing topics {topic_name} - {e}......................")
        raise e


def create_topic_if_not_exists(kafka_client, topic_name):
    """
    Create a topic in Kafka if it does not exist
    :param kafka_client: KafkaAdminClient
    :param topic_name: Name of the topic
    :return:
    """
    try:
        if is_topic_exists(kafka_client, topic_name):
            logging.info(f"Topic {topic_name} already exists......................")
            return
        logging.info(f"Creating Topic {topic_name}......................")

        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        kafka_client.create_topics(new_topics=topic_list, validate_only=False)
        logging.info(f"Kafka topic {topic_name} created successfully......................")
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
    topic_name = sys.argv[1]
    kafka_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    create_topic_if_not_exists(kafka_client, topic_name)
    produce_kafka(BOOTSTRAP_SERVERS, topic_name)


if __name__ == '__main__':
    main()
