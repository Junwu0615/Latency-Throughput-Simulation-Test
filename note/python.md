<a href='https://github.com/Junwu0615/Latency-Throughput-Simulation-Test'><img alt='GitHub Views' src='https://views.whatilearened.today/views/github/Junwu0615/Latency-Throughput-Simulation-Test.svg'> <br> 
[![](https://img.shields.io/badge/Language-GO-blue.svg?style=plastic)](https://go.dev/) 
[![](https://img.shields.io/badge/Language-Python_3.12.0-blue.svg?style=plastic)](https://www.python.org/) <br>
[![](https://img.shields.io/badge/Tools-MongoDB-yellow.svg?style=plastic)](https://www.mongodb.com/)
[![](https://img.shields.io/badge/Tools-Redis-yellow.svg?style=plastic)](https://redis.io/)
[![](https://img.shields.io/badge/Tools-Apache_Kafka-yellow.svg?style=plastic)](https://kafka.apache.org/)
[![](https://img.shields.io/badge/Tools-Docker-yellow.svg?style=plastic)](https://www.docker.com/) 

<br>

## *⭐ Python ⭐*
- ### *A.　流程運行*
```Text
*Producer → *Kafka → *Consumer → *Redis / *MongoDB

[Data Producer #Python] # producer.py [模擬高頻率資料流產生]
       │
       ▼
 [Kafka Topic: "test-data"]
       │
       ▼
[Data Consumer #Python] 
       │
       ├── Read from Kafka # consumer.py [從 Kafka 消費資料並寫入 Redis / MongoDB]
       │
   ├── Write to Redis [快取]
   └── Write to MongoDB [落地]
```

<br>

- ### *B.　Kafka*
- ![JPG](../sample/kafka_00.jpg)
- ![JPG](../sample/kafka_01.jpg)
- ![JPG](../sample/kafka_02.jpg)
- ![JPG](../sample/kafka_03.jpg)
- ![JPG](../sample/kafka_04.jpg)

<br>

- ### *C.　MongoDB*
- ![JPG](../sample/mongodb_00.jpg)
- ![JPG](../sample/mongodb_01.jpg)

<br>

- ### *D.　Redis*
- ![JPG](../sample/redis_00.jpg)
- ![JPG](../sample/redis_01.jpg)