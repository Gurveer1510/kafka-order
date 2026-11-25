package main

import (
	"context"
	"log"
	"orderflow/internal/kafka"
	"orderflow/internal/payments"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	consumeClient, err := kafka.NewKafkaClient(kgo.ConsumerGroup("payment-group"), kgo.ConsumeTopics("order.created"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	for {
		fetched := consumeClient.PollFetches(context.Background())

		for _, fetchErr := range fetched.Errors() {
			log.Printf(
				"Topic: %v | Partition: %v | Error: %v \n",
				fetchErr.Topic,
				fetchErr.Partition,
				fetchErr.Err,
			)
		}

		fetched.EachRecord(func(r *kgo.Record) {
			payments.ProcessPayment(context.Background(), consumeClient, r)
		})

	}
}
