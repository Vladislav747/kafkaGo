package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "go-kafka-client",
		"acks":              "all",
	})

	if err != nil {
		fmt.Println("Failed to create producer:", err)
	}

	delivery_chan := make(chan kafka.Event, 10000)
	topic := "HYSE"
	value := "example"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value)},
		delivery_chan,
	)

	if err != nil {
		log.Fatal(err)
	}

	e := <-delivery_chan

	fmt.Println("%+v\n", e)

	fmt.Println(p)
}
