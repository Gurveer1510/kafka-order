package events

import "time"

type PaymentEvent struct {
	PaymentID string    `json:"payment_id"`
	OrderID   string    `json:"order_id"`
	Amount    int64     `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}
