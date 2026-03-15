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
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &Producer{
		writer: writer,
		topic:  topic,
	}
}

func (p *Producer) SendMessagesWithKeys(ctx context.Context, count int) error {
	defer p.writer.Close()

	log.Println("Producer sending messages with KEYS...")

	keyTypes := []struct {
		prefix string
		values []string
	}{
		{"user", []string{"alice", "bob", "charlie"}},
		{"order", []string{"1001", "1002", "1003", "1004"}},
		{"payment", []string{"txn-001", "txn-002"}},
	}

	messageCounter := 1

	for i := 1; i <= count; i++ {
		kt := keyTypes[i%len(keyTypes)]
		keyValue := kt.values[i%len(kt.values)]
		key := fmt.Sprintf("%s-%s", kt.prefix, keyValue)

		message := fmt.Sprintf("Message #%d with key '%s' at %s",
			messageCounter, key, time.Now().Format("15:04:05.000"))

		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(message),
			Headers: []kafka.Header{
				{Key: "msg-id", Value: []byte(fmt.Sprintf("%d", messageCounter))},
				{Key: "key-type", Value: []byte(kt.prefix)},
			},
		}

		if err := p.writer.WriteMessages(ctx, msg); err != nil {
			return fmt.Errorf("failed to send message %d: %w", messageCounter, err)
		}

		fmt.Printf("Message %d sent with key: '%s'\n", messageCounter, key)
		messageCounter++
		time.Sleep(300 * time.Millisecond)
	}

	log.Printf("Producer finished. Total messages: %d", messageCounter-1)
	return nil
}

// SendMessagesWithSameKey same keys dropped in one partition
func (p *Producer) SendMessagesWithSameKey(ctx context.Context) error {
	defer p.writer.Close()

	log.Println("Producer sending messages with SAME KEY...")

	sameKey := "user-123"

	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("Message #%d with same key '%s' at %s",
			i, sameKey, time.Now().Format("15:04:05.000"))

		msg := kafka.Message{
			Key:   []byte(sameKey),
			Value: []byte(message),
			Headers: []kafka.Header{
				{Key: "seq", Value: []byte(fmt.Sprintf("%d", i))},
			},
		}

		if err := p.writer.WriteMessages(ctx, msg); err != nil {
			return fmt.Errorf("failed to send message %d: %w", i, err)
		}

		fmt.Printf("Message %d sent with key: '%s'\n", i, sameKey)
		time.Sleep(300 * time.Millisecond)
	}

	log.Println("Producer finished")
	return nil
}