from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import datetime
from src.config import BROKER1, BROKER2, LOG_TOPIC


class KafkaClient:
    def __init__(self):
        self.__producer = None
        self.__consumer = None
        self.__admin_client = None
        self.__bootstrap_servers = '{},{}'.format(BROKER1, BROKER2)

    def set_new_topic(self, topic_name: str, num_partitions: int, replication_factor: int):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.__bootstrap_servers)
            new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics(new_topics=[new_topic])
        except Exception as e:
            print('set_new_topic fail. Error is {}'.format(e))

    def close_admin_client(self):
        if self.__admin_client is not None:
            self.__admin_client.close()

    def produce_value(self, topic_name: str, value: dict):
        try:
            if self.__producer is None:
                self.__producer = KafkaProducer(bootstrap_servers=self.__bootstrap_servers,
                                                value_serializer=lambda m: json.dumps(m).encode())
            self.__producer.send(topic_name, value=value)
        except Exception as e:
            print('produce_value fail. Error is ', e)

    def close_producer(self):
        if self.__producer is not None:
            self.__producer.close()

    def consume_value(self, topic_name: str, group_id: str):
        try:
            if self.__consumer is None:
                self.__consumer = KafkaConsumer(topic_name,
                                                bootstrap_servers=self.__bootstrap_servers,
                                                group_id=group_id,
                                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                                auto_offset_reset='earliest')
            for msg in self.__consumer:
                print("key: ", msg.key)
                print("value: ", msg.value)
                return msg.key, msg.value

        except Exception as e:
            print('consume_value fail. Error is {}'.format(e))

    def send_log(self, method: str, status: bool, message: str):
        try:
            if self.__producer is None:
                self.__producer = KafkaProducer(bootstrap_servers=self.__bootstrap_servers,
                                                value_serializer=lambda m: json.dumps(m).encode())
            now = datetime.datetime.now()
            current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            log = {
                "time": current_time,
                "method": method,
                "status": status,
                "message": message
            }
            self.produce_value(LOG_TOPIC, log)
        except Exception as e:
            print('send_log fail. Error is ', e)

    def __del__(self):
        if self.__producer is not None:
            self.__producer.close()
        if self.__consumer is not None:
            self.__consumer.close()
        if self.__admin_client is not None:
            self.__admin_client.close()
