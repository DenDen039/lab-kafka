package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/websocket"
)

const (
	BitstampWSURL = "wss://ws.bitstamp.net"
)

type SubscribeMessage struct {
	Event string `json:"event"`
	Data  struct {
		Channel string `json:"channel"`
	} `json:"data"`
}

func main() {
	// Flags
	kafkaServer := flag.String("server", "localhost:9094", "Kafka's host")
	topic := flag.String("topic", "transactions", "Kafka's topic to listen")
	flag.Parse()

	// Producer creation
	baseProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServer,
		"acks":              "all"})
	if err != nil {
		log.Fatalf("Cannot create producer: %v", err)
		os.Exit(1)
	}

	deliveryChannel := make(chan kafka.Event, 10000)
	defer close(deliveryChannel)
	producer := NewCustomProducer(baseProducer, deliveryChannel)

	// Subscribe to live_orders_btcusd channel via websocket
	u := url.URL{Scheme: "wss", Host: "ws.bitstamp.net"}
	websocketConnection, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("Cannot dial websocket:", err)
		os.Exit(1)
	}
	defer websocketConnection.Close()

	err = Subscribe(websocketConnection, "live_orders_btcusd")
	if err != nil {
		log.Fatalf("Cannot subscribe channel: %v", err)
		os.Exit(1)
	}

	for {
		_, message, err := websocketConnection.ReadMessage()
		if err != nil {
			log.Println("cannot fetch message:", err)
			continue
		}

		testMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
			Value:          []byte(message)}

		_, err = producer.SyncProduceWithRetry(testMsg, 2, time.Millisecond*10)
		if err != nil {
			fmt.Printf("Kafka producer error occured: %v", err)
			return
		}
	}
}

func Subscribe(websocketConnection *websocket.Conn, subscribeChannel string) error {
	msg := SubscribeMessage{
		Event: "bts:subscribe",
		Data: struct {
			Channel string `json:"channel"`
		}{
			Channel: subscribeChannel,
		},
	}

	if err := websocketConnection.WriteJSON(msg); err != nil {
		return err
	}

	return nil
}
