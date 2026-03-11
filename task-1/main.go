package main

import (
	"kafka-broker-demo/kafka/consumer"
	"kafka-broker-demo/kafka/producer"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokerAddr = "localhost:9092"
	topicName = "test-topic"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	admin, err := sarama.NewClusterAdmin([]string{brokerAddr}, config)
	if err != nil {
		log.Fatalf("Can't create ClusterAdmin: %v", err)
	}

	defer func() { _ = admin.Close() }()

	if err = createTopic(admin, topicName); err != nil {
		log.Fatalf("Topic creation error: %v", err)
	}
	log.Printf("Topic '%s' success created or already exists.", topicName)

	go func() {
		time.Sleep(2 * time.Second)
		if err := producer.RunProducer(brokerAddr, topicName, config); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	if err := consumer.RunConsumer(brokerAddr, topicName, config); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
	log.Println("App complete")
}

func createTopic(admin sarama.ClusterAdmin, topic string) error {
	topicDetail := &sarama.TopicDetail{
		NumPartitions: 1,
		ReplicationFactor: 1,
	}
	return admin.CreateTopic(topic, topicDetail, false)
}