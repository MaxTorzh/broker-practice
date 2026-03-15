package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"task-10/internal/consumer"
	"task-10/internal/producer"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
	groupID    = "my-group"
)

func main() {
	var mode string
	flag.StringVar(&mode, "mode", "mixed", "producer mode: mixed or same-key")
	flag.Parse()

	if err := createTopic(); err != nil {
		log.Printf("Topic creation warning: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		time.Sleep(3 * time.Second)

		prod := producer.NewProducer(brokerAddr, topicName)

		var err error
		if mode == "same-key" {
			err = prod.SendMessagesWithSameKey(ctx)
		} else {
			err = prod.SendMessagesWithKeys(ctx, 15) // 15 messages with different keys
		}

		if err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	consumerGroup := consumer.NewGroup(brokerAddr, topicName, groupID, 3)

	go func() {
		if err := consumerGroup.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer group error: %v", err)
		}
	}()

	<-sigChan
	log.Println("\nReceived shutdown signal")
	cancel()
	time.Sleep(2 * time.Second)
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