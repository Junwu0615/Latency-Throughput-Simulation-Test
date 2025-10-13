import time, json, random
from kafka import KafkaProducer
from utils.logger import Logger


# TODO 初始化 logger
logger = Logger(console_name=f'.producer_console',
                file_name=f'.producer_file')


# TODO 初始化 Kafka Producer
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test-data'
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_data():
    return {
        'timestamp': time.time(),
        'device_id': f'dev-{random.randint(1, 10)}',
        'value': random.random() * 100
    }


def success_callback(metadata):
    logger.info(f'Message sent to partition {metadata.partition} @ offset {metadata.offset}')


def error_callback(exception):
    logger.error('Failed to send message')


if __name__ == '__main__':
    logger.warning('🚀 Producer started, sending messages to Kafka...')
    while True:
        data = generate_data()
        # producer.send(TOPIC, value=data)
        producer.send(TOPIC, value=data).add_callback(success_callback).add_errback(error_callback)
        # producer.flush()
        # time.sleep(0.001) # 1ms interval