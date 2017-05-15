package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Consumer struct {
	Limit int
	Amqp
	DB
}

func NewConsumer(
	limit int,
	rabbitURL string,
	queue string,
	db string,
) *Consumer {
	return &Consumer{
		Limit: limit,
		Amqp: Amqp{
			RabbitURL: rabbitURL,
			Queue:     queue,
		},
		DB: DB{
			URL: db,
		},
	}
}

func (c Consumer) store(user User) error {
	log.Println(user)
	return nil
}

func (c Consumer) Run() {
	defer c.Close()

	(&c).InitRabbit()

	// (&c).InitDB()

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

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	for i := 0; i < c.Limit; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case record := <-records:
					var user User
					log.Println(string(record.Body))
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
			}
		}()
	}

	cancelChan := make(chan os.Signal)
	signal.Notify(cancelChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-cancelChan
		log.Println("Stoping consuming")
		cancel()
	}()

	wg.Wait()
}
