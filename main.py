from src.client.redis_client import RedisClient
rc = RedisClient()
print(rc.fetch_set_elements("url"))


'''from src.client.kafka_client import KafkaClient
kc = KafkaClient()
kc.set_new_topic('log', 1, 2)'''

