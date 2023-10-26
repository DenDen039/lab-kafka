package main

import (
	"fmt"
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
