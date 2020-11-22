from datetime import datetime
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

brokers, topic = 'localhost:9092', 'test-kafka'

es = Elasticsearch(([{'host': '127.0.0.1', 'port': 9200}]))


if __name__ == '__main__':
    consumer = KafkaConsumer(topic, group_id='test-consumer-group', bootstrap_servers=[brokers])
    index_number = 0
    for msg in consumer:
        # print("key=%s, value=%s" % (msg.key, msg.value))
        doc = {
            'author': msg.value.decode("utf-8"),
            'text': 'Elasticsearch: cool. bonsai cool.',
            'timestamp': datetime.now(),
        }
        res = es.index(index="hello-index", id=index_number, body=doc)
        print(res['result'])

        res = es.get(index="hello-index", id=index_number)
        print(res['_source'])

        es.indices.refresh(index="hello-index")

        res = es.search(index="hello-index", body={"query": {"match_all": {}}})
        print("Got %d Hits:" % res['hits']['total']['value'])
        index_number = index_number + 1
#for hit in res['hits']['hits']:
#    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])