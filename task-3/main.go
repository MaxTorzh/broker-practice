package main

import (
	"flag"
	"log"
	"time"

	"task-3/kafka/consumer"
	"task-3/kafka/producer"

	"github.com/IBM/sarama"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
)

func main() {
	var mode string
	var workers int
	flag.StringVar(&mode, "mode", "group", "consumer mode: group or parallel")
	flag.IntVar(&workers, "workers", 3, "number of workers/consumers")
	flag.Parse()

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	admin, err := sarama.NewClusterAdmin([]string{brokerAddr}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	if err := createTopic(admin, topicName); err != nil {
		log.Printf("Topic creation warning: %v", err)
	} else {
		log.Printf("Topic '%s' created", topicName)
	}

	go func() {
		time.Sleep(2 * time.Second)
		configCopy := *config
		if err := producer.RunAsyncProducer(brokerAddr, topicName, &configCopy); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	switch mode {
	case "group":
		log.Printf("Consumer group starting with %d workers", workers)
		if err := consumer.RunConsumerGroup(brokerAddr, topicName, "my-group", workers, config); err != nil {
			log.Fatalf("Consumer group error: %v", err)
		}
	default:
		log.Fatalf("Unknown mode: %s", mode)
	}

	log.Println("Application finished")
}

func createTopic(admin sarama.ClusterAdmin, topic string) error {
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}
	return admin.CreateTopic(topic, topicDetail, false)
}