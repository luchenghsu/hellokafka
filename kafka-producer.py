from kafka import KafkaProducer
import time

brokers, topic = 'localhost:9092', 'test-kafka'

def start():
    while True:
        print(" --- produce ---")
        time.sleep(10)
        producer.send(topic, key=b'foo', value=b'kimochi')
        producer.flush()
 
 
if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=brokers)
    start()
    producer.close()