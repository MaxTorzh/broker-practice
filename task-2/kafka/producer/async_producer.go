package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func RunAsyncProducer(brokers string, topic string) error {
	results := make(chan struct {
		messageID int
		partition int
		offset    int64
		err       error
	}, 10)
	
	var wg sync.WaitGroup
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		successCount := 0
		
		for res := range results {
			if res.err != nil {
				fmt.Printf("Failed to send message #%d: %v\n", res.messageID, res.err)
			} else {
				successCount++
				fmt.Printf("Message #%d delivered to partition %d at offset %d\n",
					res.messageID, res.partition, res.offset)
			}
		}
		fmt.Printf("Total successful messages: %d\n", successCount)
	}()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			for _, msg := range messages {
				messageID := 0
				for _, h := range msg.Headers {
					if h.Key == "message-id" {
						fmt.Sscanf(string(h.Value), "%d", &messageID)
						break
					}
				}
				
				results <- struct {
					messageID int
					partition int
					offset    int64
					err       error
				}{
					messageID: messageID,
					partition: msg.Partition,
					offset:    msg.Offset,
					err:       err,
				}
			}
		},
	}
	defer writer.Close()

	fmt.Println("Async producer started. Sending messages...")

	ctx := context.Background()
	for i := range 10 {
		messageID := i + 1
		message := fmt.Sprintf("Async message #%d from Go at %s", messageID, time.Now().Format("15:04:05.000"))

		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(message),
			Headers: []kafka.Header{
				{Key: "message-id", Value: []byte(fmt.Sprintf("%d", messageID))},
			},
		})

		if err != nil {
			results <- struct {
				messageID int
				partition int
				offset    int64
				err       error
			}{messageID: messageID, err: err}
		}

		fmt.Printf("Message #%d queued\n", messageID)
	}

	fmt.Println("All messages queued. Waiting for confirmations...")
	
	time.Sleep(3 * time.Second)
	
	close(results)
	wg.Wait()
	
	fmt.Println("Async producer finished")
	return nil
}