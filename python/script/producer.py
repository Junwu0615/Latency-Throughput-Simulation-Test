from confluent_kafka import Producer
import time
import json

conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)

topic = "test_topic"

for i in range(5):
    data = {"id": i, "message": f"hello {i}"}
    producer.produce(topic, json.dumps(data).encode("utf-8"))
    producer.flush()
    print(f"Produced: {data}")
    time.sleep(1)