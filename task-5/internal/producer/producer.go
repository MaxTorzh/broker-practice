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
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &Producer{
		writer: writer,
		topic:  topic,
	}
}

func (p *Producer) SendMessagesToAllPartitions(ctx context.Context) error {
	defer p.writer.Close()

	log.Println("Producer sending messages to all partitions...")

	totalPartitions := 3
	messagesPerPartitions := 3
	messageCounter := 1

	for partition := 0; partition < totalPartitions; partition++ {
		log.Printf("Sending to partition %d...", partition)

		for msgNum := 1; msgNum < messagesPerPartitions; msgNum++ {
			message := fmt.Sprintf("[Partition %d] Message #%d at %s",
				partition, msgNum, time.Now().Format("15:04:05.000"))

			//Selecting exact partition via partition field
			err := p.writer.WriteMessages(ctx, kafka.Message{
				Partition: partition,
				Key:       []byte(fmt.Sprintf("key-p%d-m%d", partition, msgNum)),
				Value:     []byte(message),
				Headers: []kafka.Header{
					{Key: "partition", Value: []byte(fmt.Sprintf("%d", partition))},
					{Key: "msg-number", Value: []byte(fmt.Sprintf("%d", msgNum))},
					{Key: "global-id", Value: []byte(fmt.Sprintf("%d", messageCounter))},
				},
			})

			if err != nil {
				return fmt.Errorf("Failed to send message to partition %d: %w", partition, err)
			}

			fmt.Printf("Message %d (global) sent to partition %d (local #%d)\n",
				messageCounter, partition, msgNum)

			messageCounter++
			time.Sleep(300 * time.Millisecond)
		}
	}

	log.Printf("Producer finished. Total messages: %d", messageCounter-1)
	return nil
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

