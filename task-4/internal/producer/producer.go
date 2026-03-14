package producer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(brokers, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        true,
	}

	return &Producer{
		writer: writer,
		topic:  topic,
	}
}

func (p *Producer) SendMessages(ctx context.Context, count int) error {
	defer p.writer.Close()

	log.Printf("Producer sending %d messages...", count)

	for i := 1; i <= count; i++ {
		// Creating message with header and identification
		msg := kafka.Message{
			Key: []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("Message #%d from group consumer at %s",
				i, time.Now().Format("15:04:05.000"))),
			Headers: []kafka.Header{
				{Key: "msg-id", Value: []byte(fmt.Sprintf("%d", i))},
				{Key: "source", Value: []byte("task-4-producer")},
			},
		}

		if err := p.writer.WriteMessages(ctx, msg); err != nil {
			return fmt.Errorf("failed to send message %d: %w", i, err)
		}

		fmt.Printf("Message %d sent\n", i)
		time.Sleep(200 * time.Millisecond) 
	}

	log.Println("Producer finished")
	return nil
}
