<a href='https://github.com/Junwu0615/Latency-Throughput-Simulation-Test'><img alt='GitHub Views' src='https://views.whatilearened.today/views/github/Junwu0615/Latency-Throughput-Simulation-Test.svg'> <br> 
[![](https://img.shields.io/badge/Language-GO-blue.svg?style=plastic)](https://go.dev/) 
[![](https://img.shields.io/badge/Language-Python_3.12.0-blue.svg?style=plastic)](https://www.python.org/) <br>
[![](https://img.shields.io/badge/Tools-MongoDB-yellow.svg?style=plastic)](https://www.mongodb.com/)
[![](https://img.shields.io/badge/Tools-Redis-yellow.svg?style=plastic)](https://redis.io/)
[![](https://img.shields.io/badge/Tools-Apache_Kafka-yellow.svg?style=plastic)](https://kafka.apache.org/)
[![](https://img.shields.io/badge/Tools-Docker-yellow.svg?style=plastic)](https://www.docker.com/) 

<br>

## *⭐ Python vs Golang 語言效能差異比較 ⭐*
### *A.　測試方式*
- #### *[producer] 為期 1 分鐘不休眠傳遞訊息至 Kafka，時間到關閉程序*
- #### *[consumer] 訂閱 Kafka Topic 解析訊號後發送至 Redis & MongoDB*
- #### *期許結果 `consumer >= producer` ; 不預期結果: `consumer < producer`*

<br>

### *B.　測試指標*
- #### *吞吐量（ Throughput, msgs/sec ） # 每秒處理數據*
- #### *平均延遲（ Average Latency ）# 平均延遲 ? 秒*
- #### *P99 延遲（ P99 Latency ） # 99% 的訊息延遲超過 ? 秒*

<br>

### *C.　Python 截圖*
- #### *吞吐量 v1: 用 batch 方式塞資料 但還是需要依序等待 I/O*
- ![JPG](../sample/python_01.jpg)
  - #### *累計處理訊息 : ... msg / s*
  - #### *吞吐量 : 1941.70 msg / s*
  - #### *平均延遲 : 0.91 ms ( 0.00 s )*
  - #### *P99 延遲 : 6.51 ms ( 0.01 s )*  - 

- #### *吞吐量 v2: 導入 ThreadPoolExecutor # 多執行緒*
- ![JPG](../sample/python_02.jpg)
  - #### *累計處理訊息 : ... msg / s*
  - #### *吞吐量 : ... msg / s*
  - #### *平均延遲 : ... ms ( ... s )*
  - #### *P99 延遲 : ... ms ( ... s )*

- #### *吞吐量 v3: 異步 I/O (Asyncio)*
- ![JPG](../sample/python_03.jpg)
  - #### *累計處理訊息 : ... msg / s*
  - #### *吞吐量 : ... msg / s*
  - #### *平均延遲 : ... ms ( ... s )*
  - #### *P99 延遲 : ... ms ( ... s )*

- #### *吞吐量 v4: 水平擴展*
- ![JPG](../sample/python_04.jpg)
  - #### *累計處理訊息 : ... msg / s*
  - #### *吞吐量 : ... msg / s*
  - #### *平均延遲 : ... ms ( ... s )*
  - #### *P99 延遲 : ... ms ( ... s )*

<br>