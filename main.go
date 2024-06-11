package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type OrderProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderProducer {
	return &OrderProducer{
		producer: p,
		topic:    topic,
	}
}

func (o *OrderProducer) Produce(orderType string, size int) error {
	o.producer.Produce(msg, nil)
}

func main() {
	topic := "HYSE"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "go-kafka-client",
		"acks":              "all",
	})

	if err != nil {
		fmt.Println("Failed to create producer:", err)
	}

	go func() {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "foo",
			"auto.offset.reset": "smallest",
		})

		if err != nil {
			log.Fatal(err)
		}

		err = consumer.Subscribe(topic, nil)

		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Consumed Message on %s: %s\n", e.TopicPartition, e.Value)
			case *kafka.Error:
				fmt.Printf("Error on: %s\n", e)
			}
		}
	}()

	delivery_chan := make(chan kafka.Event, 10000)
	for {

		value := "example"
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value)},
			delivery_chan,
		)

		if err != nil {
			log.Fatal(err)
		}

		<-delivery_chan

		time.Sleep(time.Second * 3)

		fmt.Println(p)
	}

}
