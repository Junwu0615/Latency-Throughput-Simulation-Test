"""
TODO
    吞吐量 v2: 導入 ThreadPoolExecutor # 多執行緒
        - 用多個執行緒來同時執行 Redis 和 MongoDB 的 I/O 批次寫入
"""
import time, json, statistics, redis
import numpy as np
from kafka import KafkaConsumer
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from utils.logger import Logger


# TODO 初始化 logger
logger = Logger(console_name=f'.consumer_console',
                # file_name=f'.consumer_file'
                )


# TODO 初始化 Kafka Consumer
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test-data'
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest', # 從最早的 offset 開始 # 確保讀到所有訊息
    # auto_offset_reset='latest', # 從最新的 offset 開始
    # enable_auto_commit=True, # [啟動] 每隔約 5 秒自動更新 offset
    enable_auto_commit=False, # [取消] 每隔約 5 秒自動更新 offset
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

# TODO: 創建一個列表來收集數據
redis_batch_data = []
mongo_batch_data = []
# 批次大小
# BATCH_SIZE = 1000
# BATCH_SIZE = 2000
BATCH_SIZE = 3000

# TODO
#  初始化 I/O 執行緒池 ( 2 個執行緒專門負責 Redis 和 Mongo 寫入 )
#  主執行緒可繼續處理 Kafka 訊息
io_executor = ThreadPoolExecutor(max_workers=2)


def write_to_redis(data_to_write):
    # TODO Redis Pipeline 執行異步 SET
    pipe = redis_client.pipeline()
    for key, value in data_to_write:
        pipe.set(key, value)
    pipe.execute()
    redis_batch_data.clear()
    # logger.info('Redis Batch Write Complete')


def write_to_mongo(data_to_write):
    # TODO MongoDB insert_many 執行異步插入
    collection.insert_many(data_to_write)
    mongo_batch_data.clear()
    # logger.info('MongoDB Batch Write Complete')


logger.warning('Consumer started... waiting for messages')
try:
    for message in consumer:
        # 解析 Kafka 訊息
        data = message.value

        # 計算延遲
        recv_time = time.time()
        latency = recv_time - data['timestamp']
        latencies.append(latency)

        # TODO Redis 寫入方式
        # --------- 異步 ---------
        redis_batch_data.append((data['device_id'], json.dumps(data)))

        # TODO MongoDB 寫入方式
        # --------- 異步 ---------
        mongo_batch_data.append({
            'device_id': data['device_id'],
            'value': data['value'],
            'producer_ts': data['timestamp'],
            'consumer_ts': recv_time,
            'latency': latency
        })

        count += 1

        # 批次處理邏輯
        if count > 0 and count % BATCH_SIZE == 0:
            # 複製當前的緩衝區數據，避免在執行緒中被修改
            redis_data_copy = redis_batch_data.copy()
            mongo_data_copy = mongo_batch_data.copy()

            # 清空主執行緒的緩衝區，準備接收下一批數據
            redis_batch_data.clear()
            mongo_batch_data.clear()

            # 提交任務給執行緒池 (非阻塞，主執行緒立即返回)
            io_executor.submit(write_to_redis, redis_data_copy)
            io_executor.submit(write_to_mongo, mongo_data_copy)

            consumer.commit()

        # 每處理 1000 筆訊息，輸出一次統計資訊
        if count % 1000 == 0:
            elapsed = time.time() - start_time
            throughput = count / elapsed
            avg_latency = statistics.mean(latencies)
            p99_latency = np.percentile(latencies, 99)

            logger.info(f'Processed: {count} msgs | '
                        f'Throughput: {throughput:.2f} msg/s | '
                        f'Avg Latency: {avg_latency * 1000:.2f} ms ( {avg_latency:.2f} s ) | '
                        f'P99 Latency: {p99_latency * 1000:.2f} ms ( {p99_latency:.2f} s )')

            latencies.clear() # 清空 latencies 列表以重新開始下一批次的統計

except KeyboardInterrupt:
    if redis_batch_data:
        io_executor.submit(write_to_redis, redis_batch_data)

    if mongo_batch_data:
        io_executor.submit(write_to_mongo, mongo_batch_data)

    consumer.commit()

    try:
        logger.error('正在關閉 Kafka Consumer ...', exc_info=False)
        consumer.close()
        logger.warning('Kafka Consumer 已關閉 !')

    except Exception as e:
        logger.error('關閉 Kafka Consumer 時發生錯誤')

    try:
        logger.error('正在關閉 MongoDB 連線 ...', exc_info=False)
        mongo_client.close()
        logger.warning('MongoDB 連線已關閉 !')

    except Exception as e:
        logger.error('關閉 MongoDB 連線時發生錯誤')

    logger.error('正在關閉 Redis 連線 ...', exc_info=False)
    if 'redis_client' in locals() and isinstance(redis_client, redis.Redis):
        try:
            redis_client.connection_pool.disconnect()
            logger.warning('Redis 連線池已明確關閉並釋放資源 !')

        except Exception as e:
            logger.error('關閉 Redis 連線池時發生錯誤')