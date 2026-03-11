package consumer

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func RunConsumer(brokers string, topic string, baseConfig *sarama.Config) error {
	consumer, err := sarama.NewConsumer([]string{brokers}, baseConfig)
	if err != nil {
		return fmt.Errorf("Can't create consumer: %w", err)
	}

	defer func() { _ = consumer.Close() }()

	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("Can't receive partition list: %w", err)
	}

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return fmt.Errorf("ConsumePartition error for partition %d: %w", partition, err)
		}

		defer func(pc sarama.PartitionConsumer) { _ = pc.Close() }(pc)

		fmt.Printf("Consumer starts for for the partition %d. Message waiting...\n", partition)

		receivedCount := 0
		for receivedCount < 10 {
			select {
			case msg := <-pc.Messages():
				receivedCount++
				fmt.Printf("Partition %d: Received message #%d: '%s' (offset: %d)\n",
					partition, receivedCount, string(msg.Value), msg.Offset)
			case err := <-pc.Errors():
				log.Printf("Consumer error: %v", err)
			}
		}

		fmt.Printf("Consumer for partition %d read 10 messages.\n", partition)
	}
	return nil
}
