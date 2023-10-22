package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer      *kafka.Producer
	topic         string
	delivery_chan chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:      p,
		topic:         topic,
		delivery_chan: make(chan kafka.Event, 10000),
	}
}

// placeOder(op *OrderPlacer, orderType string, size int) error {
func (op *OrderPlacer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)
	err := op.producer.Produe(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		op.delivery_chan,
	)

	if err != nil {
		log.Fatal(err)
	}
	<-op.delivery_chan

	fmt.Printf("placed order on the queue %s\n", format)

	return nil
}

func main() {
	topic := "HVSE"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo", //socket.gethostname()",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	op := NewOrderProducer(p, topic)
	for i := 0; i < 1000; i++ {
		if err := op.placeOder("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}
}
