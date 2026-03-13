package main

import (
	"fmt"
	"log"
	"time"

	"task-1/kafka/consumer"
	"task-1/kafka/producer"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
)

func main() {
	// Создание топика
	if err := createTopic(); err != nil {
		log.Fatalf("Topic creation error: %v", err)
	}
	log.Printf("Topic '%s' success created or already exists.", topicName)

	// Запуск producer в фоне
	go func() {
		time.Sleep(2 * time.Second)
		if err := producer.RunProducer(brokerAddr, topicName); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	// Запуск consumer
	if err := consumer.RunConsumer(brokerAddr, topicName); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
	log.Println("App complete")
}

func createTopic() error {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	return controllerConn.CreateTopics(topicConfig)
}