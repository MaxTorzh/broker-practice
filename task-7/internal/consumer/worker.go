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
	fmt.Printf("Started %d workers\n", wp.numWorkers)
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task := <-wp.tasks:
			wp.processMessage(id, task)

			wp.mu.Lock()
			wp.processed++
			wp.mu.Unlock()

		case <-wp.quit:
			fmt.Printf("Worker %d stopped\n", id)
		}
	}
}

// Working immitation
func (wp *WorkerPool) processMessage(workerID int, task WorkerTask) {
	//Different time simulation
	processingTime := time.Duration(100+workerID*50) * time.Millisecond
	time.Sleep(processingTime)

	msgNum := "unknown"
	for _, h := range task.Message.Headers {
		if h.Key == "msg-id" {
			msgNum = string(h.Value)
			break
		}
	}

	fmt.Printf("[Worker %d] Processed: partition=%d, offset=%d, msg=%s, time=%v\n",
		workerID, task.Message.Partition, task.Message.Offset, msgNum, processingTime)
}

// Adding task to the queue
func (wp *WorkerPool) Submit(task WorkerTask) {
	select {
	case wp.tasks <- task:
	default:
		log.Printf("Worker pool queue full, message offset=%d, will wait", task.Message.Offset)
		wp.tasks <- task
	}
}

// Pool stopping
func (wp *WorkerPool) Stop() {
	fmt.Println("Stopping working pool...")
	close(wp.quit)
	wp.wg.Wait()
	close(wp.tasks)

	wp.mu.Lock()
	defer wp.mu.Unlock()
	fmt.Printf("Total processed messages: %d\n", wp.processed)
}