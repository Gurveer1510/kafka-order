package events

import "time"

type Order struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}
