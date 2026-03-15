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
		CommitInterval: 0, // 0 = cancel autocommit
		
		//Settings for the manual commit
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

	log.Printf("Consumer group started (MANUAL COMMIT mode)")

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

			//Sending in worker pool
			result := make(chan error, 1)
			g.workerPool.Submit(WorkerTask{
				Message:    msg,
				Reader:     g.reader,
				ResultChan: result, //New chan for result
			})

			select {
			case err := <-result:
				if err == nil {
					//Success: commit offset
					if err := g.reader.CommitMessages(ctx, msg); err != nil {
						log.Printf("Failed to commit offset %d: %v", msg.Offset, err)
					} else {
						log.Printf("Committed offset %d", msg.Offset)
					}
				} else {
					//Error offset commit declined
					log.Printf("Processing failed for offset %d: %v", msg.Offset, err)
				}
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (g *Group) Close() error {
	return g.reader.Close()
}