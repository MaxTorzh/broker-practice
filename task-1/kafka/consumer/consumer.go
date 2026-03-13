package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func RunConsumer(brokers string, topic string) error {
	// reader config
	config := kafka.ReaderConfig{
		Brokers:     []string{brokers},
		Topic:       topic,
		Partition:   0,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	partition := 0
	fmt.Printf("Consumer starts fot the partition %d. Message waiting...\n", partition)

	ctx := context.Background()
	receivedCount := 0

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Consumer error: %v", err)
			break
		}
		receivedCount++
		fmt.Printf("Partition %d: Received message #%d: '%s' (offset: %d)\n",
			msg.Partition, receivedCount, string(msg.Value), msg.Offset)

			if receivedCount >= 10 {
			break
		}
	}
	fmt.Printf("Consumer for partition %d read 10 messages.\n", partition)
	return nil
}
