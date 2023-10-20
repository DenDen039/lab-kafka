package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	kafkaServer := flag.String("server", "localhost:9094", "Kafka's host")
	topic := flag.String("topic", "transactions", "Kafka's topic to listen")

	flag.Parse()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServer,
		"auto.offset.reset": "smallest"})
	if err != nil {
		log.Fatalf("Cannot setup consumer: %v", err)
	}
	defer consumer.Close()

	consumer.Subscribe(*topic, nil)
	for {
		event := consumer.Poll(100)
		switch e := event.(type) {
		case *kafka.Message:
			fmt.Println(string(e.Value))
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			break
		}
	}

}
