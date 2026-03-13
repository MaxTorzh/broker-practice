package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func RunConsumerGroup(brokers string, topic string, groupID string, numWorkers int) error {
	config := kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	workerPool := NewWorkerPool(numWorkers, 100)
	workerPool.Start()

	fmt.Printf("Partitions are assigned and consumer group '%s' started with %d workers\n", groupID, numWorkers)

	// Создание контекста с возможностью отмены
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Запуск consumer в горутине
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Shutting down consumer...")
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if err == context.Canceled {
						return
					}
					log.Printf("Error reading message: %v", err)
					continue
				}
				workerPool.Submit(WorkerTask{
					Message: msg,
					Reader:  reader,
				})
			}
		}
	}()

	// Ожидание сигнала остановки
	<-sigChan
	fmt.Println("\nReceived shutdown signal")
	
	// Отмена контекста
	cancel()
	
	// Остановка worker pool
	workerPool.Stop()
	
	fmt.Println("Consumer group stopped gracefully")
	return nil
}