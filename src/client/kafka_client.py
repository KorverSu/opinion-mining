from src.config import BROKER1, BROKER2
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class KafkaClient:
    def __init__(self):
        self.__producer = None
        self.__consumer = None
        self.__admin_client = None
        self.__bootstrap_servers = '{},{}'.format(BROKER1, BROKER2)

    def get_admin_client(self):
        try:
            conf = {'bootstrap.servers': self.__bootstrap_servers}
            self.__admin_client = AdminClient(conf)
            return self
        except Exception as e:
            print('get_admin_client fail. Error is {}'.format(e))

    def set_new_topic(self, topic_name: str, num_partitions: int, replication_factor: int):
        try:
            new_topic = NewTopic(topic_name, num_partitions, replication_factor)
            self.__admin_client.create_topics([new_topic])
        except Exception as e:
            print('set_new_topic fail. Error is {}'.format(e))

    def get_producer(self):
        try:
            conf = {'bootstrap.servers': self.__bootstrap_servers}
            self.__producer = Producer(conf)
            return self
        except Exception as e:
            print('get_producer fail. Error is {}'.format(e))

    def produce_value(self, topic_name: str, key: str, value: str):
        try:
            self.__producer.produce(topic_name, key=key, value=value, callback=delivery_report, )
            self.__producer.poll(10000)
            self.__producer.flush()
        except Exception as e:
            print('produce_value fail. Error is {}'.format(e))

    def get_consumer(self, group_id: str):
        try:
            conf = {'bootstrap.servers': self.__bootstrap_servers, 'group.id': group_id}
            self.__consumer = Consumer(conf)
            return self
        except Exception as e:
            print('get_consumer fail. Error is {}'.format(e))

    def consume_value(self, topic_name: str):
        self.__consumer.subscribe([topic_name])
        try:
            while True:
                msg = self.__consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('Reached end of partition, committing offsets...')
                        self.__consumer.commit(msg)
                    else:
                        print('Error while consuming message: {}'.format(msg.error()))
                else:
                    print('Received message: key={}, value={}'.format(msg.key(), msg.value()))
                    self.__consumer.commit(msg)
        except Exception as e:
            print('consume_value fail. Error is {}'.format(e))

