package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TransactionsProducer struct {
	producer        *kafka.Producer
	deliveryChannel chan kafka.Event
}

func NewCustomProducer(kafkaProducer *kafka.Producer, deliveryChannel chan kafka.Event) *TransactionsProducer {
	return &TransactionsProducer{
		producer:        kafkaProducer,
		deliveryChannel: deliveryChannel,
	}
}

func (p *TransactionsProducer) SyncProduce(message *kafka.Message) (*kafka.Message, error) {
	err := p.producer.Produce(message, p.deliveryChannel)
	if err != nil {
		return nil, err
	}

	eventResponse := <-p.deliveryChannel
	messageResponse := eventResponse.(*kafka.Message)
	if messageResponse.TopicPartition.Error != nil {
		return nil, fmt.Errorf("Delivery failed: %v\n", message.TopicPartition.Error)
	}

	return messageResponse, nil
}

func (p *TransactionsProducer) SyncProduceWithRetry(
	message *kafka.Message,
	retries int,
	pollingTime time.Duration,
) (*kafka.Message, error) {
	var err error
	var messageResponse *kafka.Message

	for i := 0; i < retries; i++ {
		messageResponse, err = p.SyncProduce(message)
		if err == nil {
			return messageResponse, nil
		}
		time.Sleep(pollingTime)
	}

	return nil, err
}

func main() {
	kafkaServer := flag.String("server", "localhost:9094", "Kafka's host")
	topic := flag.String("topic", "transactions", "Kafka's topic to listen")
	flag.Parse()

	baseProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServer,
		"acks":              "all"})
	if err != nil {
		log.Fatalf("Cannot create producer: %v", err)
		return
	}

	deliveryChannel := make(chan kafka.Event, 10000)
	defer close(deliveryChannel)
	producer := NewCustomProducer(baseProducer, deliveryChannel)

	for i := 0; ; i++ {
		// TODO Implement web socket
		time.Sleep(time.Second)

		testMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
			Value:          []byte(fmt.Sprintf("foo%v", i))}

		_, err := producer.SyncProduceWithRetry(testMsg, 2, time.Second)
		if err != nil {
			fmt.Printf("Error occured: %v", err)
			return
		}
	}

}
