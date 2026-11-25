package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"orderflow/internal/events"
	"orderflow/internal/kafka"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type OrderHandler struct {
	kc *kgo.Client
}

func NewOrderHandler(c *kgo.Client) *OrderHandler {
	return &OrderHandler{
		kc: c,
	}
}

func (h *OrderHandler) PublishOrderEventHandler(w http.ResponseWriter, r *http.Request) {
	var order events.Order

	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid body"))
		return
	}
	id := uuid.New()
	order.OrderID = id.String()
	order.CreatedAt = time.Now().UTC()

	if order.UserID == "" || order.ProductID == "" || order.Quantity <= 0 || order.Amount <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid order data"))
		return
	}

	payload, err := json.Marshal(order)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	err = kafka.PublishEvent(ctx, "order.created", h.kc, []byte(order.OrderID), payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": order.OrderID,
		"status": "accepted",
	})
}
