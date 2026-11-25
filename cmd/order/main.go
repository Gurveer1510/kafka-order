package main

import (
	"fmt"
	"log"
	"net/http"
	"orderflow/internal/kafka"
	"orderflow/internal/order/handler"

	"github.com/go-chi/chi/v5"
)

func main() {
	producerClient, err := kafka.NewKafkaClient()
	if err != nil {
		log.Println(err)
		return
	}

	router := chi.NewRouter()
	h := handler.NewOrderHandler(producerClient)

	router.Post("/orders", h.PublishOrderEventHandler)

	fmt.Println("Server started at localhost:8080")
	if err := http.ListenAndServe("localhost:8080", router); err != nil {
		panic(err)
	}
}
