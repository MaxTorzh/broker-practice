package main

import (
	"fmt"
	"log"
	"time"

	"task-2/kafka/consumer"
	"task-2/kafka/producer"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
)

func main() {
	// Создание топика
	if err := createTopic(); err != nil {
		log.Printf("Topic creation warning: %v", err)
	} else {
		log.Printf("Topic '%s' created", topicName)
	}

	// Запуск асинхронного producer
	go func() {
		time.Sleep(2 * time.Second)
		if err := producer.RunAsyncProducer(brokerAddr, topicName); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	// Запуск consumer
	if err := consumer.RunConsumer(brokerAddr, topicName); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	log.Println("Application finished")
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