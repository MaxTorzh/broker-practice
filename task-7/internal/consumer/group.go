package consumer

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Group struct {
	reader     *kafka.Reader
	workerPool *WorkerPool
	topic      string
}

// NewGroup creating new consumer group
func NewGroup(brokers, topic, groupID string, workers int) *Group {
	config := kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,

		//Setting for group stability
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
		MaxAttempts:       3,
	}

	//Reader creating
	reader := kafka.NewReader(config)

	//Creating Worker Pool
	workerPool := NewWorkerPool(workers, 100)

	return &Group{
		reader:     reader,
		workerPool: workerPool,
		topic:      topic,
	}
}

// Run consumer group (blocking operation)
func (g *Group) Run(ctx context.Context) error {
	//Brokers starting
	g.workerPool.Start()
	defer g.workerPool.Stop()

	log.Printf("Consumer group joined. Waiting for messages...")

	//Main reading cycle
	for {
		select {
		case <-ctx.Done():
			//Receive stopping signal
			log.Println("Stopping consumer group...")
			return nil

		default:
			//Reading messages (blocking before receiving or error)
			msg, err := g.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("Reading error: %v", err)
				continue
			}
			//Sending message to worker pool
			g.workerPool.Submit(WorkerTask{
				Message: msg,
				Reader:  g.reader,
			})
		}
	}
}

func (g *Group) Close() error {
	return g.reader.Close()
}
