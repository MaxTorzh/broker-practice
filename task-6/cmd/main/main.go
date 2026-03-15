package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	//Setting via flags
	var (
		broker      string
		topic       string
		fromBegin   bool
		showKey     bool
		showHeaders bool
	)

	flag.StringVar(&broker, "broker", "localhost:9092", "Kafka broker address")
	flag.StringVar(&topic, "topic", "test-topic", "Kafka topic name")
	flag.BoolVar(&fromBegin, "from-beginning", false, "Read from beginning")
	flag.BoolVar(&showKey, "show-key", false, "Show message key")
	flag.BoolVar(&showHeaders, "show-headers", false, "Show message headers")
	flag.Parse()

		//Reader creating
	config := kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
	}

	if fromBegin {
		config.StartOffset = kafka.FirstOffset
	} else {
		config.StartOffset = kafka.LastOffset
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	fmt.Printf("\nConsole Consumer Started\n")
	fmt.Printf("Topic: %s\n", topic)
	fmt.Printf("Broker: %s\n", broker)
	fmt.Printf("Reading from: %s\n", map[bool]string{true: "BEGINNING", false: "NOW"}[fromBegin])
	fmt.Printf("Show key: %v, Show headers: %v\n", showKey, showHeaders)
	fmt.Println("────────────────────────────────────────────────────")
	fmt.Println("Waiting for messages... (Press Ctrl+C to stop)")
	fmt.Println()

	//ctx for stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//chan for signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	//chan for messages
	msgChan := make(chan kafka.Message, 100)
	errChan := make(chan error, 1)

	go func() {
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				errChan <- err
				return
			}
			msgChan <- msg
		}
	}()

	for {
		select {
		case <-sigChan:
			fmt.Println("\n\nShutting down...")
			return

		case err := <-errChan:
			if ctx.Err() == nil {
				log.Printf("Error: %v", err)
			}
			return

		case msg := <-msgChan:
			//Receiving time
			fmt.Printf("[%s] ", time.Now().Format("15:04:05.000"))

			//Partition an offset
			fmt.Printf("p%d@%d ", msg.Partition, msg.Offset)

			//Key
			if showKey && len(msg.Key) > 0 {
				fmt.Printf("key=%s ", string(msg.Key))
			}

			//Header
			if showHeaders && len(msg.Headers) > 0 {
				fmt.Printf("headers=")
				for i, h := range msg.Headers {
					if i > 0 {
						fmt.Print(",")
					}
					fmt.Printf("%s=%s", h.Key, string(h.Value))
				}
				fmt.Print(" ")
			}

			//Message
			fmt.Printf("msg=%s\n", string(msg.Value))
		}
	}
}