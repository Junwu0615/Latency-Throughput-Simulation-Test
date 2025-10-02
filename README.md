<a href='https://github.com/Junwu0615/Latency-Throughput-Simulation-Test'><img alt='GitHub Views' src='https://views.whatilearened.today/views/github/Junwu0615/Latency-Throughput-Simulation-Test.svg'> <br> 
[![](https://img.shields.io/badge/Language-GO-blue.svg?style=plastic)](https://go.dev/) 
[![](https://img.shields.io/badge/Language-Python_3.12.0-blue.svg?style=plastic)](https://www.python.org/) <br>
[![](https://img.shields.io/badge/Tools-MongoDB-yellow.svg?style=plastic)](https://www.mongodb.com/)
[![](https://img.shields.io/badge/Tools-Redis-yellow.svg?style=plastic)](https://redis.io/)
[![](https://img.shields.io/badge/Tools-Apache_Kafka-yellow.svg?style=plastic)](https://kafka.apache.org/)
[![](https://img.shields.io/badge/Tools-Docker-yellow.svg?style=plastic)](https://www.docker.com/) 

<br>

## *⭐ Latency-Throughput-Simulation-Test ⭐*

### *A.　Current Progress*
|項目|敘述|完成時間|
|:--:|:--:|:--:|
| 專案上架 | - | 2025-10-02 |
| 新增 README | - | 2025-10-02 |
| Docker 啟動必要服務 | - | 2025-10-02 |
| 專案實作說明 | - | 2025-10-02 |
| Python 實作流程 | - | - |
| GO 實作流程 | - | - |
| 語言效能差異比較 | - | - |


<br>

### *B.　Docker Build*
- #### *進入路徑 & 創建持久化空間*
  ```bash
  cd docker
  md mongo_data; md redis_data; md redis_insight_data;
  ```
  
- #### *啟動 docker-compose*
  ```bash
  docker-compose up -p latency_throughput_simulation_test -d
  ```

- #### *檢視服務是否正確啟用*
  ```bash
  docker ps -a
  ```

- #### *關閉服務*
  ```bash
  docker-compose down
  ```
  
- ![PNG](./sample/docker-compose%20up.PNG)