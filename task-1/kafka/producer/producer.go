package producer

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

func RunProducer(brokers string, topic string, baseConfig *sarama.Config) error {

	producer, err := sarama.NewSyncProducer([]string{brokers}, baseConfig)
	if err != nil {
		return fmt.Errorf("Can't create a producer: %w", err)
	}

	defer func() { _ = producer.Close() }() // закрытие producer для освобождения ресурсов

	fmt.Println("Producer starts, sending messages...")

	for i := range 10 {
		message := fmt.Sprintf("Hello, Kafka! Message #%d at %s", i+1, time.Now().Format("15:04:05.000"))
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("Sending message error %d: %w", i, err)
		}

		fmt.Printf("Message %d send to: partition %d, offset %d\n", i, partition, offset)
	}

	fmt.Println("Producer finished work")
	return nil
}
