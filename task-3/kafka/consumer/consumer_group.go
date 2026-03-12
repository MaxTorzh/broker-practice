package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type GroupHandler struct {
	ready      chan bool
	workerPool *WorkerPool // для параллельной обработки внутри партиции
}

func (h *GroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(h.ready)
	fmt.Printf("Partitions are assigned: %v\n", session.Claims())
	return nil
}

func (h *GroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Session is closed")
	return nil
}

func (h *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Каждая партиция работает в своей горутине
	for message := range claim.Messages() {
		// worker pool для параллельной обработки внутри партиции
		h.workerPool.Submit(WorkerTask{
			Message: message,
			Session: session,
		})
	}
	return nil
}

func RunConsumerGroup(brokers string, topic string, groupID string, numWorkers int, config *sarama.Config) error {
	// Настройки для consumer group
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	client, err := sarama.NewConsumerGroup([]string{brokers}, groupID, config)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	defer client.Close()

	workerPool := NewWorkerPool(numWorkers, 100)
	workerPool.Start()

	handler := &GroupHandler{
		ready:      make(chan bool),
		workerPool: workerPool,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("Error from consumer group: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	<-handler.ready
	fmt.Printf("Consumer group '%s' started with %d workers\n", groupID, numWorkers)

	// Ждем сигнала завершения
	time.Sleep(30 * time.Second)

	workerPool.Stop()
	fmt.Println("Consumer group stopped")

	return nil

}
