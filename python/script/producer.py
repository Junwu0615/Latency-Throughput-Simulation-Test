import time, json, random
from kafka import KafkaProducer
from utils.logger import Logger


# TODO 初始化 logger
logger = Logger(console_name=f'.producer_console',
                # file_name=f'.producer_file'
                )


# TODO 初始化 Kafka Producer
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test-data'
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    linger_ms=5, # 延遲 5ms 再發送，保證批次穩定
    batch_size=32 * 1024, # 32KB
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_data():
    return {
        'timestamp': time.time(),
        'device_id': f'dev-{random.randint(1, 10)}',
        'value': random.random() * 100
    }


def success_callback(metadata):
    # logger.info(f'Message sent to partition {metadata.partition} @ offset {metadata.offset}')
    pass


def error_callback(exception):
    logger.error(f'Failed to send message: {exception}')


if __name__ == '__main__':
    logger.warning('Producer started, sending messages to Kafka... 1 min closedown')
    start_time = time.time()
    try:
        # while True:
        while start_time + 60 > time.time():
            data = generate_data()
            # producer.send(TOPIC, value=data)
            producer.send(TOPIC, value=data).add_callback(success_callback).add_errback(error_callback)
            # producer.flush() # flush() 會阻塞 Producer，破壞了 Kafka 內建的批次處理。移除可以讓 Producer 以非同步方式發送數據包。
            # time.sleep(0.0001) # 硬性限制 0.1 ms interval
            # time.sleep(0.00001) # 硬性限制 0.01 ms interval
            # time.sleep(0.00004) # 硬性限制 0.04 ms interval

    finally:
        try:
            logger.error('正在關閉 Kafka Producer ...', exc_info=False)
            producer.flush()
            time.sleep(2)  # 傳輸最後緩衝的時間
            producer.close()
            logger.warning('Kafka Producer 已關閉 ...')

        except Exception as e:
            logger.error('關閉 Kafka Producer 時發生錯誤')