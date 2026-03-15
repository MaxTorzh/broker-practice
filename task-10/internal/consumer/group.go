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

func NewGroup(brokers, topic, groupID string, workers int) *Group {
	config := kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 5 * time.Second, // ← autocommit every 5 sec
		
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
		MaxAttempts:       3,
	}

	reader := kafka.NewReader(config)
	workerPool := NewWorkerPool(workers, 100)

	return &Group{
		reader:     reader,
		workerPool: workerPool,
		topic:      topic,
	}
}

func (g *Group) Run(ctx context.Context) error {
	g.workerPool.Start()
	defer g.workerPool.Stop()

	log.Printf("Consumer group started (AUTO COMMIT mode, interval=5s)")

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping consumer group...")
			return nil

		default:
			msg, err := g.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("Read error: %v", err)
				continue
			}

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