package main

type Consumer struct {
	Limit     int
	RabbitURL string
	Queue     string
}

func NewConsumer(limit int, rabbitURL string, queue string) *Consumer {
	return &Consumer{Limit: limit, RabbitURL: rabbitURL, Queue: queue}
}

func (c Consumer) Run() {
	return
}
