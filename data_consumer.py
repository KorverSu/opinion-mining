from src.es_writer.kafka_es_writer import KafkaElasticsearchWriter
if __name__ == '__main__':
    kw = KafkaElasticsearchWriter()
    kw.run()
