package producer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func RunAsyncProducer(brokers string, topic string, config *sarama.Config) error {
	producer, err := sarama.NewAsyncProducer([]string{brokers}, config)
	if err != nil {
		return fmt.Errorf("Failed to create async producer: %w", err)
	}

	var wg sync.WaitGroup

	successDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for success := range producer.Successes() {
			count++
			fmt.Printf("Message #%v delivered to partition %d at offset %d\n",
				success.Metadata, success.Partition, success.Offset)
		}
		fmt.Printf("Total successful messages: %d\n", count)
		close(successDone)
	}()

	errorDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for err := range producer.Errors() {
			count++
			log.Printf("Failed to send message #%v: %v", err.Msg.Metadata, err.Err)
		}
		fmt.Printf("Total failed messages: %d\n", count)
		close(errorDone)
	}()

	fmt.Println("Async producer started. Sending messages...")

	for i := range 10 {
		message := fmt.Sprintf("Async message #%d from Go at %s", i+1, time.Now().Format("15:04:05.000"))

		msg := &sarama.ProducerMessage{
			Topic:    topic,
			Value:    sarama.StringEncoder(message),
			Metadata: i + 1,
		}

		// Асинхронная отправка - не блокируется
		producer.Input() <- msg

		fmt.Printf("Message #%d queued\n", i+1)
	}

	fmt.Println("All messages queued. Waiting for confirmations...")
	producer.AsyncClose()

	wg.Wait()

	// Проверка, что оба обработчика завершились
	<-successDone
	<-errorDone

	fmt.Println("Async producer finished")
	return nil
}
