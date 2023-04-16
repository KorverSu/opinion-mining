'''from kafka.admin import KafkaAdminClient, NewTopic

# 设置 Kafka 集群中的 bootstrap.servers
bootstrap_servers = 'localhost:29092,localhost:39092'

# 创建 KafkaAdminClient 实例
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# 定义一个新主题

new_topic = NewTopic(name='bb', num_partitions=1, replication_factor=2)

# 创建新主题
admin_client.create_topics(new_topics=[new_topic])
admin_client.close()'''

