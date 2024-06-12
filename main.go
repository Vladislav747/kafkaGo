package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type OrderPlacer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:     p,
		topic:        topic,
		deliveryChan: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		op.deliveryChan,
	)

	if err != nil {
		log.Fatal(err)
	}

	<-op.deliveryChan

	return nil

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

	op := NewOrderPlacer(p, topic)

	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(3 * time.Second)
	}

}
