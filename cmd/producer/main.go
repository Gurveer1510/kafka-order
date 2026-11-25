package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"orderflow/internal/events"
	"orderflow/internal/kafka"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	kClient, err := kafka.NewKafkaClient()
	if err != nil {
		log.Println(err)
		return
	}
	defer kClient.Close()

	// var rcrds []kgo.Record
	fmt.Println("Partition | Offset | Timestamp")
	for i := 1; i <= 10; i++ {
		order := events.Order{
			OrderID:   fmt.Sprintf("order-%d", i),
			UserID:    "user-101",
			ProductID: "product-201",
			Quantity:  2,
			Amount:    50000,
			CreatedAt: time.Now().UTC(),
		}
		payload, err := json.Marshal(order)
		if err != nil {
			log.Print(err)
			continue
		}
		record := &kgo.Record{
			Topic: "order.created",
			Key:   []byte(order.OrderID),
			Value: payload,
		}
		result := kClient.ProduceSync(context.Background(), record)
		if err := result.FirstErr(); err != nil {
			log.Println(err)
			continue
		}

		fmt.Printf("%v | %v | %v\n", record.Partition, record.Offset, record.Timestamp)
	}

}
