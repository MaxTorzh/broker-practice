package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"task-4/internal/consumer"
	"task-4/internal/producer"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
	groupID    = "my-group"
)

func main() {

	if err := createTopic(); err != nil {
		log.Printf("Topic creation warning: %v", err)
	}

	// Creting context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel for OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		time.Sleep(3 * time.Second)

		prod := producer.NewProducer(brokerAddr, topicName)
		if err := prod.SendMessages(ctx, 10); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	consumerGroup := consumer.NewGroup(brokerAddr, topicName, groupID, 3)

	go func() {
		if err := consumerGroup.Run(ctx); err != nil {
			log.Printf("Consumer group error: %v", err)
		}
	}()

	<-sigChan
	log.Println("\n Received shutdown signal")

	//Graceful shutdown
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("Application finished")
}

func createTopic() error {
	// Connecting to kafka
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

	// Creating topic with 3 partitions
	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	err = controllerConn.CreateTopics(topicConfig)
	if err != nil {
		return err
	}

	log.Printf("Topic '%s' created with 3 partitions", topicName)
	return nil
}
