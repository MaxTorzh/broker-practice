package consumer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type WorkerTask struct {
	Message kafka.Message
	Reader  *kafka.Reader
}

type WorkerPool struct {
	tasks      chan WorkerTask
	wg         sync.WaitGroup
	quit       chan struct{}
	numWorkers int
	processed  int64
	failed     int64
	mu         sync.Mutex
}

func NewWorkerPool(numWorkers int, bufferSize int) *WorkerPool {
	return &WorkerPool{
		tasks:      make(chan WorkerTask, bufferSize),
		quit:       make(chan struct{}),
		numWorkers: numWorkers,
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	fmt.Printf("Started %d workers (auto-commit mode)\n", wp.numWorkers)
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task := <-wp.tasks:
			success := wp.processMessage(id, task.Message)
			
			wp.mu.Lock()
			if success {
				wp.processed++
			} else {
				wp.failed++
			}
			wp.mu.Unlock()

		case <-wp.quit:
			return
		}
	}
}

func (wp *WorkerPool) processMessage(workerID int, msg kafka.Message) bool {
	processingTime := time.Duration(100+workerID*50) * time.Millisecond
	time.Sleep(processingTime)

	msgID := "unknown"
	for _, h := range msg.Headers {
		if h.Key == "msg-id" {
			msgID = string(h.Value)
			break
		}
	}

	var isError bool
	if msgID == "3" || msgID == "6" || msgID == "9" {
		isError = true
	}

	keyStr := "no key"
	if len(msg.Key) > 0 {
		keyStr = string(msg.Key)
	}

	if !isError {
		fmt.Printf("[Worker %d] partition=%d, offset=%d, key=%s, msg=%s, time=%v\n",
			workerID, msg.Partition, msg.Offset, keyStr, msgID, processingTime)
	} else {
		fmt.Printf("[Worker %d] FAILED partition=%d, offset=%d, key=%s, msg=%s\n",
			workerID, msg.Partition, msg.Offset, keyStr, msgID)
	}

	return !isError
}

func (wp *WorkerPool) Submit(task WorkerTask) {
	select {
	case wp.tasks <- task:
	default:
		log.Printf("Worker pool queue full, message offset=%d will wait", task.Message.Offset)
		wp.tasks <- task
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.quit)
	wp.wg.Wait()
	close(wp.tasks)

	wp.mu.Lock()
	defer wp.mu.Unlock()
	fmt.Printf("Total processed: %d successful, %d failed (auto-committed)\n", 
		wp.processed, wp.failed)
}