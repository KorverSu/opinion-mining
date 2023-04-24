import time

from src.client.kafka_client import KafkaClient
from src.client.elasticsearch_client import ElasticsearchClient


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
            value = self.consume_data('url', 'qwe')
            if value is None:
                time.sleep(1)
                continue
            print("consumer")
            self.write_data('news', value)
            print(value)


if __name__ == '__main__':
    kew = KafkaElasticsearchWriter()
    kew.run()

