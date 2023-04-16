'''import json
import time
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:29092,localhost:39092',
                         value_serializer=lambda m: json.dumps(m).encode())

for i in range(2):
    data = {'num': i, 'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    producer.send('qwe', data)
    time.sleep(1)'''
import datetime
import time

from src.client.kafka_client import KafkaClient

kc = KafkaClient()
for i in range(2):
    data = {'num': i, 'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    kc.produce_value('cc', data)
    time.sleep(1)

kc.close_producer()

