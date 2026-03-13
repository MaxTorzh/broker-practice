package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)
func RunProducer(brokers string, topic string) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()

	fmt.Println("Producer starts, sending messages...")

	ctx := context.Background()
	for i := range 10 {
		message := fmt.Sprintf("Hello, Kafka! Message #%d at %s", i+1, time.Now().Format("15:04:05.000"))
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(message),
		})
		if err != nil {
			return fmt.Errorf("sending message error %d: %w", i, err)
		}
		fmt.Printf("Message %d send to: partition %d, offset %d\n",
			i, 0, i)
	}
	fmt.Println("Producer finished work")
	return nil
}
