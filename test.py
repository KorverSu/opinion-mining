from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# 设置 Kafka 集群中的 bootstrap.servers
def run():

    conf = {'bootstrap.servers': 'localhost:29092,localhost:39092'}


    # 创建 Kafka 生产者
    producer = Producer(conf)

    # 发送消息
    producer.produce('test2', key='my-key', value='Hello, Kafka!', callback=delivery_report, )


    # 等待所有消息都被发送（默认异步发送）
    producer.poll(10000)
    producer.flush()

    print(123)

def a(x=3):
    print(x)

a()
a(5)
a()

