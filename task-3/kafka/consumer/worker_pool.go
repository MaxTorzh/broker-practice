package consumer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// WorkerTask представляет задачу на обработку сообщения
type WorkerTask struct {
	Message *sarama.ConsumerMessage
	Session sarama.ConsumerGroupSession
}

// WorkerPool управляет пулом воркеров для параллельной обработки
type WorkerPool struct {
	tasks      chan WorkerTask
	wg         sync.WaitGroup
	quit       chan struct{}
	numWorkers int
	processing sync.Map
}

// NewWorkerPool создает новый пул воркеров
func NewWorkerPool(numWorkers int, bufferSize int) *WorkerPool {
	return &WorkerPool{
		tasks:      make(chan WorkerTask, bufferSize),
		quit:       make(chan struct{}),
		numWorkers: numWorkers,
	}
}

// Start запускает воркер
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	fmt.Printf("Starting #%d workers for reading messages\n", wp.numWorkers)
}

// worker - отдельный обработчик сообщений
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task := <-wp.tasks:
			key := fmt.Sprintf("%d-%d", task.Message.Partition, task.Message.Offset)
			wp.processing.Store(key, time.Now())

			wp.processingMessage(id, task)

			wp.processing.Delete(key)
			task.Session.MarkMessage(task.Message, "")

		case <-wp.quit:
			fmt.Printf("Worker %d ends to work\n", id)
			return
		}
	}
}

// processMessage имитирует бизнес-логику обработки сообщения
func (wp *WorkerPool) processingMessage(workerID int, task WorkerTask) {
	msg := task.Message

	// Симуляция разного времени обработки для наглядности
	processingTime := time.Duration(100+workerID*50) * time.Millisecond
	time.Sleep(processingTime)

	fmt.Printf("[Worker %d] Message processed: partition=%d, offset=%d, value=%s (time: %v)\n",
		workerID, msg.Partition, msg.Offset, string(msg.Value), processingTime)
}

func (wp *WorkerPool) Submit(task WorkerTask) {
	select {
	case wp.tasks <- task:
	default:
		log.Printf("Worker queue is overloaded! Message offset=%d will be processed later", task.Message.Offset)
		wp.tasks <- task // все равно отправится, но это заблокируется
	}
}

// Stop останавливает пул воркеров
func (wp *WorkerPool) Stop() {
	fmt.Println("WorkerPool was stopped...")
	close(wp.quit)
	wp.wg.Wait()
	close(wp.tasks)

	// Статистика незавершенных задач
	var inProgress []string
	wp.processing.Range(func(key, value interface{}) bool {
		inProgress = append(inProgress, fmt.Sprintf("%s (with %v)", key, value))
		return true
	})

	if len(inProgress) > 0 {
		fmt.Printf("There are tasks in progress: %v\n", inProgress)
	}
}

// GetStats возвращает статистику пула
func (wp *WorkerPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"num_workers": wp.numWorkers,
		"queue_size":  len(wp.tasks),
		"capacity":    cap(wp.tasks),
	}
}
