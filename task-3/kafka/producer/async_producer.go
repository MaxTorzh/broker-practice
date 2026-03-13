package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func RunAsyncProducer(brokers string, topic string) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 50 * time.Millisecond,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				fmt.Printf("Batch failed: %v\n", err)
				return
			}
			// Вывод информации о каждом сообщении в батче
			for _, msg := range messages {
				// Извлекаем messageID из заголовков
				messageID := "unknown"
				for _, h := range msg.Headers {
					if h.Key == "message-id" {
						messageID = string(h.Value)
						break
					}
				}
				fmt.Printf("Message #%s delivered to partition %d at offset %d\n",
					messageID, msg.Partition, msg.Offset)
			}
		},
	}
	defer writer.Close()

	fmt.Println("Async producer started. Sending messages...")

	var wg sync.WaitGroup
	ctx := context.Background()

	// Отправляем сообщения с разными ключами для распределения по партициям
	for i := range 10 {
    message := fmt.Sprintf("Async message #%d from Go at %s", 
        i, time.Now().Format("15:04:05.000"))
    
    err := writer.WriteMessages(ctx, kafka.Message{
        Key:   []byte(fmt.Sprintf("key-%d", i)),
        Value: []byte(message),
        Headers: []kafka.Header{
            {Key: "message-id", Value: []byte(fmt.Sprintf("%d", i))},
        },
    })
    
    if err != nil {
        fmt.Printf("Failed to queue message #%d: %v\n", i, err)
    } else {
        fmt.Printf("Message #%d queued\n", i)
    }
    time.Sleep(100 * time.Millisecond)
}

	wg.Wait()
	fmt.Println("All messages queued. Waiting for confirmations...")
	time.Sleep(2 * time.Second)
	fmt.Println("Async producer finished")
	return nil
}