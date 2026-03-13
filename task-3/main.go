package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"task-3/kafka/consumer"
	"task-3/kafka/producer"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
)

func main() {
	var workers int
	flag.IntVar(&workers, "workers", 3, "number of workers")
	flag.Parse()

	// Создание топика с 3 партициями
	if err := createTopic(); err != nil {
		log.Printf("Topic creation warning: %v", err)
	} else {
		log.Printf("Topic '%s' created with 3 partitions", topicName)
	}

	// Запуск producer
	go func() {
		time.Sleep(2 * time.Second)
		if err := producer.RunAsyncProducer(brokerAddr, topicName); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	// Запуск consumer group
	log.Printf("Consumer group starting with %d workers", workers)
	if err := consumer.RunConsumerGroup(brokerAddr, topicName, "my-group", workers); err != nil {
		log.Fatalf("Consumer group error: %v", err)
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
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	return controllerConn.CreateTopics(topicConfig)
}