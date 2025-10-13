package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/montanaflynn/stats" // 用於 P99 統計
	"github.com/segmentio/kafka-go"
)

// 訊息結構，對應 Producer 發送的 JSON
type SensorData struct {
	Timestamp float64 `json:"timestamp"` // Producer 傳送時間 (秒級)
	DeviceID  string  `json:"device_id"`
	Value     float64 `json:"value"`
}

// --- 配置參數 ---
const (
	KAFKA_BROKER = "localhost:9092"
	TOPIC        = "test-data"
	GROUP_ID     = "go-consumer"
	WORKER_COUNT = 8    // 8 個 Goroutine 負責 JSON 解析和計算
	BATCH_SIZE   = 1000 // 統計批次大小
)

// --- 處理器 (Worker) ---
// 負責解析 JSON、計算延遲，並將延遲結果發送給統計器
func worker(id int, msgs <-chan []byte, results chan<- float64) {
	var data SensorData
	for msgBytes := range msgs {
		recvTimeNano := time.Now().UnixNano() // Go 接收時間 (納秒)

		// 1. JSON 反序列化 (CPU 密集)
		if err := json.Unmarshal(msgBytes, &data); err != nil {
			log.Printf("Worker %d: JSON Unmarshal error: %v", id, err)
			continue
		}

		// 2. 計算延遲 (使用納秒級精度)
		// Producer 發送時間 (秒級 float) * 10^9 轉為納秒級整數
		sendTimeNano := int64(data.Timestamp * 1e9)
		latencyNano := recvTimeNano - sendTimeNano

		// 將延遲結果 (秒級 float) 發送給統計器
		results <- float64(latencyNano) / 1e9
	}
	log.Printf("Worker %d: Shutting down...", id)
}

// --- 統計器 (Stats Aggregator) ---
// 負責收集延遲數據、計算並定期輸出吞吐量和 P99 延遲
func statsAggregator(results <-chan float64) {
	var latencies []float64
	var totalProcessed int64 = 0
	startTime := time.Now()

	log.Println("Stats Aggregator started.")

	for latency := range results {
		latencies = append(latencies, latency)
		totalProcessed++

		if len(latencies) >= BATCH_SIZE {
			// 計算吞吐量
			elapsed := time.Since(startTime).Seconds()
			throughput := float64(totalProcessed) / elapsed

			// 計算 P99 延遲 (返回兩個值: float64, error)
			p99Latency, err := stats.Percentile(latencies, 99)
			if err != nil {
				p99Latency = 0.0
			}

			// *** 修正開始 ***
			// 計算平均延遲 (返回兩個值: float64, error)
			avgLatency, err := stats.Mean(latencies)
			if err != nil {
				avgLatency = 0.0 // 如果計算失敗，平均值設為 0
			}
			// *** 修正結束 ***

			// 輸出結果 (現在使用修正後的 avgLatency 變數)
			log.Printf("Processed: %d msgs | Throughput: %.2f msg/s | Avg Latency: %.2f ms | P99 Latency: %.2f ms",
				totalProcessed,
				throughput,
				avgLatency*1000, // 使用 avgLatency
				p99Latency*1000,
			)

			// 重置計數器 (不重置總計數和開始時間，以計算總平均)
			latencies = nil
		}
	}
	log.Println("Stats Aggregator finished final calculation and shut down.")
}

func main() {
	log.Println("🚀 Go Consumer started... waiting for messages")

	// 1. 設置 Context 和信號處理
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	// 監聽 Ctrl+C (SIGINT) 和 終止信號 (SIGTERM)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 啟動一個 Goroutine 來等待結束信號
	go func() {
		<-sigChan // 阻塞直到收到信號
		log.Println("Received termination signal (Ctrl+C). Cancelling Context...")
		cancel() // 收到信號後，取消主讀取迴圈的 Context
	}()

	// 2. 初始化 Kafka Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{KAFKA_BROKER},
		Topic:          TOPIC,
		GroupID:        GROUP_ID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset, // 從最早的 offset 開始讀取
	})
	// 確保在 main 函式結束時關閉 Reader (無論是正常還是異常退出)
	defer r.Close()

	// 3. 設置併發結構
	messageChannel := make(chan []byte, 1000) // 訊息緩衝區
	resultChannel := make(chan float64, 1000) // 結果緩衝區

	// 啟動統計 Goroutine (非阻塞)
	go statsAggregator(resultChannel)

	// 啟動 Worker Pool
	var wg sync.WaitGroup
	for i := 1; i <= WORKER_COUNT; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(workerID, messageChannel, resultChannel)
		}(i)
	}

	// 4. --- 主讀取迴圈 ---
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// **** 關鍵修正在這裡 ****
			if errors.Is(err, context.Canceled) {
				// 收到 context canceled 錯誤，表示我們主動發出了退出信號
				log.Println("Kafka Reader Context cancelled. Exiting read loop...")
				break // 退出主讀取迴圈，進入優雅關閉流程
			}
			// 處理其他實際的連線錯誤
			log.Printf("Error reading message: %v", err)
			break
		}
		// 將訊息發送到 Worker Channel
		messageChannel <- m.Value
	}

	// 5. 優雅關閉流程
	log.Println("Waiting for workers to finish current batch...")

	// 關閉 messageChannel，通知所有 Worker 協程 (range msgs) 退出
	close(messageChannel)

	// 等待所有 Worker 協程透過 wg.Done() 完成工作並退出
	wg.Wait()

	// 關閉 resultChannel，通知 statsAggregator 協程 (range results) 退出並輸出最終統計
	close(resultChannel)

	// 最終退出
	log.Println("All Goroutines finished. Go Consumer gracefully shut down.")
}
