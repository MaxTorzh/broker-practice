package consumer

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type WorkerTask struct {
	Message    kafka.Message
	Reader     *kafka.Reader
	ResultChan chan error //New chan for result
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
	fmt.Printf("Started %d workers (manual commit mode)\n", wp.numWorkers)
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task := <-wp.tasks:
			err := wp.processMessage(id, task.Message)
			
			if task.ResultChan != nil {
				task.ResultChan <- err
				close(task.ResultChan)
			}

			wp.mu.Lock()
			if err == nil {
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

func (wp *WorkerPool) processMessage(workerID int, msg kafka.Message) error {
	processingTime := time.Duration(100+workerID*50) * time.Millisecond
	time.Sleep(processingTime)

	msgID := "unknown"
	for _, h := range msg.Headers {
		if h.Key == "msg-id" {
			msgID = string(h.Value)
			break
		}
	}

	//Simulating error for demonstration, every 3rd msg with 'order' key will be error
	var processingErr error
	if msgID == "3" || msgID == "6" || msgID == "9" {
		processingErr = errors.New("simulated processing error")
	}

	keyStr := "no key"
	if len(msg.Key) > 0 {
		keyStr = string(msg.Key)
	}

	if processingErr == nil {
		fmt.Printf("[Worker %d] partition=%d, offset=%d, key=%s, msg=%s, time=%v\n",
			workerID, msg.Partition, msg.Offset, keyStr, msgID, processingTime)
	} else {
		fmt.Printf("[Worker %d] FAILED partition=%d, offset=%d, key=%s, msg=%s, error=%v\n",
			workerID, msg.Partition, msg.Offset, keyStr, msgID, processingErr)
	}

	return processingErr
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
	fmt.Printf("Total processed: %d successful, %d failed\n", wp.processed, wp.failed)
}