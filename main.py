'''from src.client.redis_client import RedisClient
rc = RedisClient()
print(rc.fetch_set_elements("url"))'''
from elasticsearch import Elasticsearch

'''from src.client.kafka_client import KafkaClient
kc = KafkaClient()
kc.set_new_topic('log', 1, 2)'''

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
es.indices.create(index='test', ignore=400)
es.index(index='test', doc_type='test', id=1, body={'test': 'test'})
print(es.get(index='test', doc_type='test', id=1)['_source'])
