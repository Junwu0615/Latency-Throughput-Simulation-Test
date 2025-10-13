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
| Docker 啟動必要服務環境 | - | 2025-10-02 |
| 新增說明文件 | - | 2025-10-13 |
| Python 實作流程 | - | 2025-10-13 |
| 語言效能差異比較 | - | 2025-10-14 |
| GO 實作流程 | - | - |

<br>

### *B.　技術棧*
|類別|技術|說明|
|:--:|:--:|:--:|
| 測試主體 | Go + Python | 比較語言在資料流處理的效能 |
| 消息中介層 | Kafka | 模擬高頻率資料流進入系統 |
| 快取層 | Redis | 測試即時資料存取效能 |
| 儲存層 | MongoDB | 模擬落地儲存的瓶頸與延遲 |
| 吞吐量 | 同步轉異步 | `單筆/同步 ( Sync ) I/O` 轉換為 `批次/異步 ( Batch/Async ) I/O` |

<br>

### *C.　文件*
- #### *[Docker 啟動必要服務環境](./note/docker.md)*
- #### *[Python 實作流程](./note/python.md)*
- #### *[GO 實作流程](./note/go.md)*
- #### *[Python vs Golang 語言效能差異比較 (不含數據庫)](./note/vs.md)*
- #### *[Python vs Golang 語言效能差異比較 (含數據庫)](./note/vs_db.md)*

<br>