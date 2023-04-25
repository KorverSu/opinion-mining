import time

from src.client.kafka_client import KafkaClient
from src.client.elasticsearch_client import ElasticsearchClient
from src.config import CRAWLER_RESULT_TOPIC, ES_RESULT_INDEX, ES_LOG_INDEX, LOG_TOPIC


class KafkaElasticsearchWriter:
    def __init__(self):
        self.__kafka_client = KafkaClient()
        self.__es_client = ElasticsearchClient()

    def consume_data(self, topic, group_id):
        value = self.__kafka_client.consume_value(topic, group_id)
        return value

    def write_data(self, index, body):
        self.__es_client.index_document(index, body)

    def run(self):
        while True:
            value = self.consume_data(CRAWLER_RESULT_TOPIC, 'qwe')
            log = self.consume_data(LOG_TOPIC, 'qwe')
            if value is None and log is None:
                time.sleep(1)
                continue
            if value is not None:
                self.write_data(ES_RESULT_INDEX, value)
            if log is not None:
                self.write_data(ES_LOG_INDEX, log)
            # print(value)
