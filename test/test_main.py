from kafka_publish.main import is_topic_exists, create_topic_if_not_exists
import unittest
from kafka.admin import KafkaAdminClient

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"


class TestMain(unittest.TestCase):
    def test_is_topic_exists(self):
        output = is_topic_exists(KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER), "test1")
        self.assertEqual(output, False)

    def test_create_topic_if_not_exists(self):
        kafka_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
        create_topic_if_not_exists(kafka_client, "test2")
        output = is_topic_exists(kafka_client, "test2")
        self.assertEqual(output, True)
        kafka_client.delete_topics(["test2"])


if __name__ == "__main__":
    TestMain.run()
