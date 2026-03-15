package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.RoundRobin{},
	}
	defer writer.Close()

	for i := range 10 {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i%3)),
			Value: []byte(fmt.Sprintf("Test message #%d", i)),
			Headers: []kafka.Header{
				{Key: "seq", Value: []byte(fmt.Sprintf("%d", i))},
			},
		}

		writer.WriteMessages(context.Background(), msg)
		fmt.Printf("Sent %d\n", i)
		time.Sleep(500 * time.Millisecond)
	}
}
