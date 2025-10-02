from confluent_kafka import Consumer
from pymongo import MongoClient
import redis
import json

# Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(["test_topic"])

# MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["test_db"]
collection = db["messages"]

# Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

print("Consumer started, waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))
        print("Consumed:", data)

        # 寫入 MongoDB
        collection.insert_one(data)

        # 寫入 Redis (用 id 當 key)
        redis_client.set(f"msg:{data['id']}", data['message'])

except KeyboardInterrupt:
    print("Stopped by user")

finally:
    consumer.close()