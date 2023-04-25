from src.client.redis_client import RedisClient
from src.client.kafka_client import KafkaClient
from src.client.elasticsearch_client import ElasticsearchClient


def initial_redis():
    rc = RedisClient()
    rc.add_hash_value("selenium_nodes", "localhost:4445", "idle")
    rc.add_hash_value("selenium_nodes", "localhost:4446", "idle")


def initial_kafka():
    kc = KafkaClient()
    kc.set_new_topic('news', 1, 2)
    kc.set_new_topic('log', 1, 2)


def initial_es():
    ec = ElasticsearchClient()
    settings = {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 2
        }
    }
    news_mappings = {
        "properties": {
            "title": {
                "type": "text"
            },
            "release_time": {
                "type": "text"
            },
            "contents": {
                "type": "text"
            }
        }
    }
    log_mappings = {
        "properties": {
            "time": {
                "type": "text"
            },
            "method": {
                "type": "text"
            },
            "status": {
                "type": "boolean"
            },
            "message": {
                "type": "text"
            }
        }
    }
    ec.create_index('news', settings, news_mappings)
    ec.create_index('log', settings, log_mappings)


def initial():
    initial_redis()
    initial_kafka()
    initial_es()
