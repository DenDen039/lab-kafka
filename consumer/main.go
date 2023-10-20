package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Transaction struct {
	Data struct {
		ID             int64   `json:"id"`
		IDStr          string  `json:"id_str"`
		OrderType      int     `json:"order_type"`
		Datetime       string  `json:"datetime"`
		MicroTimestamp string  `json:"microtimestamp"`
		Amount         float64 `json:"amount"`
		AmountStr      string  `json:"amount_str"`
		Price          int64   `json:"price"`
		PriceStr       string  `json:"price_str"`
	} `json:"data"`
	Channel string `json:"channel"`
	Event   string `json:"event"`
}

func main() {
	kafkaServer := flag.String("server", "localhost:9094", "Kafka's host")
	topic := flag.String("topic", "transactions", "Kafka's topic to listen")
	groupID := flag.String("group", "0", "Consumer's group id")
	flag.Parse()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServer,
		"group.id":          *groupID,
		"auto.offset.reset": "smallest"})
	if err != nil {
		log.Fatalf("Cannot setup consumer: %v", err)
		os.Exit(1)
	}
	defer consumer.Close()
	consumer.Subscribe(*topic, nil)

	fmt.Println("Top 10 transactions by price:")
	topTransactions := &TransactionsHeap{}

	for {
		event := consumer.Poll(100)
		switch e := event.(type) {
		case *kafka.Message:
			transaction := &Transaction{}
			json.Unmarshal(e.Value, transaction)

			if topTransactions.AddElement(transaction) {
				prettyPrint(getHeapOrder(topTransactions))
			}
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			return
		}
	}

}

func clearScreen() {
	switch currentOS := runtime.GOOS; currentOS {
	case "windows":
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		cmd.Run()
	case "darwin", "linux":
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		cmd.Run()
	default:
		// Fallback for other (unhandled) OSes
		print("\033[H\033[2J")
	}
}

func prettyPrint(transactions []*Transaction) {
	clearScreen()

	fmt.Println("Top 10 transactions by price:")
	for _, transaction := range transactions {
		fmt.Printf("ID: %s, Price: %d, Amount, %f Type: %d TimeStamp: %s\n",
			transaction.Data.IDStr,
			transaction.Data.Price,
			transaction.Data.Amount,
			transaction.Data.OrderType,
			transaction.Data.Datetime)
	}
}
