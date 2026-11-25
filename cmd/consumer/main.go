package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"orderflow/internal/events"
	"orderflow/internal/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	group := flag.String("group", "payment-group", "Consumer group name")
	flag.Parse()
	kClient, err := kafka.NewKafkaClient(kgo.ConsumerGroup(*group), kgo.ConsumeTopics("order.created"),kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Println(err)
		return
	}
	defer kClient.Close()

	fmt.Println("OrderId | Partition | Offset | Amount")
	for {
		fetched := kClient.PollFetches(context.Background())

		fetched.EachRecord(func(rcrd *kgo.Record) {
			// fmt.Println(string(rcrd.Key))
			// fmt.Println(string(rcrd.Value))
			order := events.Order{}
			err := json.Unmarshal(rcrd.Value, &order)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("%v | %v | %v | %v\n", order.OrderID, rcrd.Partition, rcrd.Offset, order.Amount)
		})
	}

}
