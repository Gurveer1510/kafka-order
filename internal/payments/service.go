package payments

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"orderflow/internal/events"
	"orderflow/internal/kafka"
	"time"
	"uuid"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProcessPayment(ctx context.Context, client *kgo.Client, rcrd *kgo.Record) {
	var order events.Order

	// key := rcrd.Key
	payload := rcrd.Value

	err := json.Unmarshal(payload, &order)
	if err != nil {
		log.Println("Unmarshal error:", err)
	}
	time.Sleep(5 * time.Millisecond)

	payment := events.PaymentEvent{
		PaymentID: uuid.New().String(),
		OrderID:   order.OrderID,
		Amount:    order.Amount,
		CreatedAt: time.Now().UTC(),
	}

	if paymentStatus() {
		payment.Status = "completed"
		key := payment.PaymentID
		payload, err := json.Marshal(payment)
		if err != nil {
			log.Println(err)
		}
		err = kafka.PublishEvent(ctx, "payment.completed", client, []byte(key), payload)
		if err != nil {
			log.Println(err)
		}
	} else {
		payment.Status = "failed"
		key := payment.PaymentID
		payload, err := json.Marshal(payment)
		if err != nil {
			log.Println(err)
		}
		err = kafka.PublishEvent(ctx, "payment.failed", client, []byte(key), payload)
		if err != nil {
			log.Println(err)
		}
	}
}

func paymentStatus() bool {
	return rand.IntN(100) < 80
}
