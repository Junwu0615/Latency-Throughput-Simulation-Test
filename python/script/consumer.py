import time, json, statistics, redis
from kafka import KafkaConsumer
from pymongo import MongoClient
from utils.logger import Logger


# TODO 初始化 logger
logger = Logger(console_name=f'.consumer_console',
                file_name=f'.consumer_file')


# TODO 初始化 Kafka Consumer
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test-data'
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='python-consumer',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)


# TODO 初始化 Redis
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redis_pass'

redis_client = None

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=0
    )
    redis_client.ping()
    logger.warning('Redis 測試連接成功並通過驗證！')

except redis.exceptions.AuthenticationError as e:
    logger.error('驗證失敗：請檢查密碼是否正確')

except Exception as e:
    logger.error('Redis 連接或操作失敗')


# TODO 初始化 MongoDB
MONGO_USERNAME = 'mongo_user'
MONGO_PASSWORD = 'mongo_pass'
MONGO_HOST = 'localhost:27017'
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}/"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client['perf_test'] # 應用程式資料庫

try:
    db.command('ping')
    logger.warning("MongoDB 連接並驗證成功！")

except Exception as e:
    logger.error('MongoDB 連接或驗證失敗')

collection = db['records']


# TODO 消費 Kafka 訊息並寫入 Redis 和 MongoDB
latencies = []
count = 0
start_time = time.time()

logger.warning('🔥 Consumer started... waiting for messages')
for message in consumer:
    # 解析 Kafka 訊息
    data = message.value

    # 計算延遲
    recv_time = time.time()
    latency = recv_time - data['timestamp']
    latencies.append(latency)

    # 寫入 Redis (以 device_id 為 key)
    redis_client.set(data['device_id'], json.dumps(data))

    # 寫入 MongoDB
    collection.insert_one({
        'device_id': data['device_id'],
        'value': data['value'],
        'producer_ts': data['timestamp'],
        'consumer_ts': recv_time,
        'latency': latency
    })

    count += 1
    if count % 1000 == 0:
        elapsed = time.time() - start_time
        throughput = count / elapsed
        avg_latency = statistics.mean(latencies)
        logger.info(f'Processed: {count} msgs | '
              f'Throughput: {throughput:.2f} msg/s | '
              f'Avg Latency: {avg_latency*1000:.2f} ms')