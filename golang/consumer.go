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
	// 移除 GROUP_ID，因為我們現在是手動讀取 Partition，不參與 Consumer Group 自動分配
	// GROUP_ID = "go-consumer"

	// WORKER_COUNT 現在作為 Partition 的數量，同時也是 CPU Worker 的數量
	WORKER_COUNT        = 8
	BATCH_SIZE          = 1000   // 統計批次大小，與 Python 測試的 1000 保持一致
	CHANNEL_BUFFER_SIZE = 100000 // 【修正】將緩衝區大小增大，防止 I/O 阻塞
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

		// 2. 計算延遲 (Latency)
		// 將 Go 的接收時間 (納秒) 轉換為秒，然後計算延遲
		recvTimeSec := float64(recvTimeNano) / float64(time.Second)
		latency := recvTimeSec - data.Timestamp

		// 3. 發送結果
		results <- latency
	}
	log.Printf("Worker %d: Exiting...", id)
}

// --- 統計器 (Stats Aggregator) ---
// 負責彙總延遲、計算吞吐量、P99 延遲並輸出
func statsAggregator(results <-chan float64) {
	var (
		latencies      []float64
		totalProcessed int64
		startTime      = time.Now().UnixNano() // 程式啟動時間
	)

	for latency := range results {
		latencies = append(latencies, latency)
		totalProcessed++

		// 每 BATCH_SIZE 輸出一次統計資訊
		if totalProcessed%BATCH_SIZE == 0 {
			// 將納秒轉換為秒
			currentTime := time.Now().UnixNano()
			elapsedTime := float64(currentTime-startTime) / float64(time.Second)

			// 計算吞吐量
			throughput := float64(totalProcessed) / elapsedTime

			// 計算平均延遲 (Average Latency)
			avgLatency := 0.0
			for _, l := range latencies {
				avgLatency += l
			}
			avgLatency /= float64(len(latencies))

			// 計算 P99 延遲
			p99Latency, err := stats.Percentile(latencies, 99)
			if err != nil {
				p99Latency = 0.0 // 錯誤處理
			}

			// 輸出結果 (與您的格式保持一致)
			log.Printf("Processed: %d msgs | Throughput: %.2f msg/s | Avg Latency: %.2f ms | P99 Latency: %.2f ms",
				totalProcessed,
				throughput,
				avgLatency*1000,
				p99Latency*1000)

			// 重設狀態
			latencies = nil // 清空 latencies
		}
	}
	log.Println("Stats Aggregator: Exiting...")
}

// 【新增/修正】針對單一 Partition 的讀取 Goroutine (I/O 併行化)
func partitionReader(ctx context.Context, partitionID int, messageChannel chan<- []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	// 1. 初始化 Partition 專屬的 Kafka Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{KAFKA_BROKER},
		Topic:   TOPIC,
		// 🔴 關鍵修正: 讀取指定 Partition 時，不設置 GROUP_ID，手動指定 Partition
		Partition:   partitionID,
		StartOffset: kafka.FirstOffset,

		MinBytes: 10e3, // 10KB (批次讀取設定)
		MaxBytes: 10e6, // 10MB
		// 【修正】將 MaxWait 降低，讓 I/O 呼叫更積極
		MaxWait: 100 * time.Millisecond,
	})
	defer r.Close()

	log.Printf("Partition Reader %d: Started reading from Partition %d...", partitionID, partitionID)

	// 2. 主讀取迴圈
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("Partition Reader %d: Context cancelled. Exiting read loop...", partitionID)
				return // 退出 Goroutine
			}
			// 避免因暫時性錯誤 (如連線中斷) 而導致程式退出
			log.Printf("Partition Reader %d: Error reading message: %v", partitionID, err)
			time.Sleep(time.Second)
			continue
		}

		// 3. 將訊息發送到 Worker Channel (Channel 緩衝區很大，這裡幾乎不會阻塞)
		messageChannel <- m.Value
	}
}

func main() {
	log.Println("🚀 Go Consumer started... waiting for messages")

	// 1. 設置 Context 和信號處理
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received termination signal (Ctrl+C). Cancelling Context...")
		cancel() // 觸發所有 Goroutine 退出
	}()

	// 2. 設置併發結構
	messageChannel := make(chan []byte, CHANNEL_BUFFER_SIZE) // 訊息緩衝區
	resultChannel := make(chan float64, CHANNEL_BUFFER_SIZE) // 結果緩衝區

	// 啟動統計 Goroutine
	go statsAggregator(resultChannel)

	// 3. 啟動 Worker Pool (CPU 密集處理): 8 個 Worker
	var wgWorker sync.WaitGroup
	for i := 1; i <= WORKER_COUNT; i++ {
		wgWorker.Add(1)
		go func(workerID int) {
			defer wgWorker.Done()
			worker(workerID, messageChannel, resultChannel)
		}(i)
	}

	// 4. 🚀 啟動 8 個 Partition Reader Goroutine (I/O 密集讀取)
	var wgReader sync.WaitGroup
	// 假設 Partition ID 從 0 開始到 7 (共 8 個)
	for i := 0; i < WORKER_COUNT; i++ {
		wgReader.Add(1)
		// 傳遞 wgReader 和 ctx
		go partitionReader(ctx, i, messageChannel, &wgReader)
	}

	// 5. 等待所有 Reader 結束 (當信號觸發 cancel 時)
	wgReader.Wait()

	// 6. 優雅關閉流程
	log.Println("All Partition Readers shut down. Waiting for workers to finish current batch...")

	// 關閉 messageChannel，通知所有 Worker 協程 (range msgs) 退出
	close(messageChannel)

	// 等待所有 Worker 協程完成工作並退出
	wgWorker.Wait()

	// 關閉 resultChannel，通知 statsAggregator 協程退出
	close(resultChannel)

	log.Println("All Goroutines finished. Go Consumer gracefully shut down.")
}
