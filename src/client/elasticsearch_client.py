"""
The class for the Elasticsearch client.
"""
from elasticsearch import Elasticsearch
from src.config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT


class ElasticsearchClient:
    def __init__(self, host=ELASTICSEARCH_HOST, port=ELASTICSEARCH_PORT):
        self.es = Elasticsearch(
            [
                {
                    'host': str(host),
                    'port': port,
                    'scheme': "http"
                }
            ]
        )

    def create_index(self, index_name, settings=None, mappings=None):
        body = {}
        if settings:
            body['settings'] = settings
        if mappings:
            body['mappings'] = mappings
        self.es.indices.create(index=index_name, body=body)

    def delete_index(self, index_name):
        self.es.indices.delete(index=index_name)

    def index_document(self, index_name, body):
        self.es.index(index=index_name, body=body)

    def search(self, index_name, query):
        return self.es.search(index=index_name, body=query)

    def delete_document(self, index_name, doc_id):
        self.es.delete(index=index_name, id=doc_id)

    def update_document(self, index_name, doc_id, body):
        self.es.update(index=index_name, id=doc_id, body=body)

    def get_all_index(self):
        return self.es.indices.get_alias().keys()

    def get_all_document(self, index_name):
        return self.es.search(index=index_name, body={"query": {"match_all": {}}})['hits']['hits']


if __name__ == '__main__':
    es = ElasticsearchClient()
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
    # es.create_index('news', settings, news_mappings)
    # es.create_index('log', settings, log_mappings)
    print(es.get_all_document('news')[0]['_source'])
    print('ok')
