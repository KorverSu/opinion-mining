from confluent_kafka.admin import AdminClient, NewTopic

conf = {'bootstrap.servers': 'localhost:29092,localhost:39092'}

# Create an AdminClient using the provided configuration
admin_client = AdminClient(conf)

# Define the new topic name, number of partitions, and replication factor
topic_name = "test2"
num_partitions = 1
replication_factor = 2

# Create a new topic with the specified number of partitions and replication factor
new_topic = NewTopic(topic_name, num_partitions, replication_factor)
admin_client.create_topics([new_topic])

