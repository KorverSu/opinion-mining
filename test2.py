from confluent_kafka import Consumer, KafkaError

# 设置 Kafka 集群中的 bootstrap.servers
conf = {'bootstrap.servers': 'localhost:29092,localhost:39092', 'group.id': 'my-python_example_group_1'}

# 创建 Kafka 消费者
consumer = Consumer(conf)

# 订阅名为 test 的 Topic
consumer.subscribe(['test2'])


while True:
    # 持续读取消息
    msg = consumer.poll(timeout=1.0)

    # 如果消息存在
    if msg is None:
        continue

    # 如果出现错误
    if msg.error():
        # 检测是不是因为 Kafka partition 没有被分配
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition, committing offsets...')
            # 没有更多的消息了，提交偏移量
            consumer.commit(msg)
        else:
            print('Error while consuming message: {}'.format(msg.error()))
    else:
        # 成功读取到一条消息
        print('Received message: key={}, value={}'.format(msg.key(), msg.value()))
        # 提交偏移量
        consumer.commit(msg)
