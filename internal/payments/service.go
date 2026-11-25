package payments

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"orderflow/internal/events"
	"orderflow/internal/kafka"
	"time"

	"github.com/google/uuid"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProcessPayment(ctx context.Context, client *kgo.Client, rcrd *kgo.Record) error {
	var order events.Order

	// key := rcrd.Key
	payload := rcrd.Value

	err := json.Unmarshal(payload, &order)
	if err != nil {
		return err
	}
	if order.OrderID == "" || order.Amount <= 0 {
		return errors.New("invalid order event")
	}
	time.Sleep(500 * time.Millisecond)

	payment := events.PaymentEvent{
		PaymentID: uuid.New().String(),
		OrderID:   order.OrderID,
		Amount:    order.Amount,
		CreatedAt: time.Now().UTC(),
	}

	topic := "payment.failed"
	payment.Status = "failed"

	if paymentStatus() {
		topic = "payment.completed"
		payment.Status = "completed"
	}

	paymentPayload, err := json.Marshal(payment)
	if err != nil {

		return err
	}
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = kafka.PublishEvent(
		publishCtx,
		topic,
		client,
		[]byte(payment.OrderID),
		paymentPayload,
	)
	if err != nil {

		return err
	}
	return nil
}

func paymentStatus() bool {
	return rand.IntN(100) < 80
}
