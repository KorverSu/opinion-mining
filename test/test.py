'''from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('my_topic',
                         bootstrap_servers='localhost:29092,localhost:39092',
                         group_id='qwe',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest')
for msg in consumer:
    print(msg.value)'''

'''from src.client.kafka_client import KafkaClient

kc = KafkaClient()
kc.consume_value('url', 'qwe')'''

class test():
    def __init__(self, a=3, b=3):
        self.a = a
        self.b = b


print(test().b)

