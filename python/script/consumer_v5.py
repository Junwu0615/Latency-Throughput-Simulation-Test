"""
TODO
    吞吐量 v5: 不含資料庫
"""
import time, json, statistics
import numpy as np
from kafka import KafkaConsumer
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


# TODO 消費 Kafka 訊息並寫入 Redis 和 MongoDB
latencies = []
count = 0
start_time = time.time()

# 批次大小
# BATCH_SIZE = 1000
# BATCH_SIZE = 2000
BATCH_SIZE = 3000

logger.warning('Consumer started... waiting for messages')
try:
    for message in consumer:
        # 解析 Kafka 訊息
        data = message.value

        # 計算延遲
        recv_time = time.time()
        latency = recv_time - data['timestamp']
        latencies.append(latency)

        count += 1

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
    consumer.commit()
    try:
        logger.error('正在關閉 Kafka Consumer ...', exc_info=False)
        consumer.close()
        logger.warning('Kafka Consumer 已關閉 !')

    except Exception as e:
        logger.error('關閉 Kafka Consumer 時發生錯誤')