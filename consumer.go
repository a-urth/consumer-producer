package main

import (
	"encoding/json"
	"log"
	"sync"
)

type Consumer struct {
	Limit int
	Amqp
}

func NewConsumer(limit int, rabbitURL string, queue string) *Consumer {
	return &Consumer{
		Limit: limit,
		Amqp: Amqp{
			RabbitURL: rabbitURL,
			Queue:     queue,
		},
	}
}

func (c Consumer) store(user User) error {
	return nil
}

func (c Consumer) Run() {
	defer c.Close()

	(&c).InitRabbit()

	records, err := c.AmqpChannel.Consume(
		c.Queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	check(err)

	var wg sync.WaitGroup
	for i := 0; i < c.Limit; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for record := range records {
				var user User
				err := json.Unmarshal(record.Body, &user)
				if err != nil {
					log.Printf("Corrupted data found %v", err)
					record.Reject(false)
					continue
				}

				err = c.store(user)
				if err != nil {
					log.Printf("Error during record store, %v", err)
					record.Reject(true)
				} else {
					record.Ack(false)
				}
			}
		}()
	}
}
