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
        # print(es.get_all_document('news')[0]['_source'])
        return self.es.search(index=index_name, body={"query": {"match_all": {}}})['hits']['hits']
