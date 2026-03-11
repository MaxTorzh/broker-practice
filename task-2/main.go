package main

import (
	"log"
	"task-2/kafka/consumer"
	"task-2/kafka/producer"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokerAddr = "localhost:9092"
	topicName  = "test-topic"
)

func main() {
	// Настройка конфигурации
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	// Для асинхронного producer нужно включить опции
	config.Producer.Return.Successes = true // получать подтверждения об успехе
	config.Producer.Return.Errors = true    // получать ошибки

	// Настройки для батчинга
	config.Producer.Flush.Frequency = 300 * time.Millisecond // отправлять пачку каждые 300мс
	config.Producer.Flush.Messages = 5                       // или когда накопится 5 сообщений

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

	// Запуск асинхронного producer в отдельной горутине
	go func() {
		time.Sleep(2 * time.Second) // даем consumer время на запуск
		if err := producer.RunAsyncProducer(brokerAddr, topicName, config); err != nil {
			log.Printf("Producer error: %v", err)
		}
	}()

	// Запуск consumer в главной горутине
	if err := consumer.RunConsumer(brokerAddr, topicName, config); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	log.Println("Application finished")
}

func createTopic(admin sarama.ClusterAdmin, topic string) error {
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	return admin.CreateTopic(topic, topicDetail, false)
}