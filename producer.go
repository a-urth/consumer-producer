package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

type Producer struct {
	Limit int
	Amqp
}

func NewProducer(limit int, rabbitURL string, queue string) *Producer {
	return &Producer{
		Limit: limit,
		Amqp: Amqp{
			RabbitURL: rabbitURL,
			Queue:     queue,
		},
	}
}

func (p Producer) send(record []string) error {
	user := User{
		Email:    record[0],
		FullName: record[1],
	}

	body, err := json.Marshal(user)
	if err != nil {
		return err
	}

	return p.AmqpChannel.Publish(
		"",
		p.Queue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
}

func (p Producer) readRecords() [][]string {
	filePath := flag.Arg(0)

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	check(err)

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	check(err)

	return records
}

func (p Producer) Run() {
	defer p.Close()

	(&p).InitRabbit()

	records := p.readRecords()

	ctx, cancel := context.WithCancel(context.Background())
	recordsChan := make(chan []string, p.Limit)

	var wg sync.WaitGroup

	go func() {
		log.Printf("Found %v records", len(records))

		defer close(recordsChan)

		for _, record := range records {
			recordsChan <- record
		}
	}()

	for i := 0; i < p.Limit; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case record, ok := <-recordsChan:
					if !ok {
						return
					}

					err := p.send(record)
					if err != nil {
						log.Printf("Error during send, retrying.\n%v", err)
						recordsChan <- record
					}
				}
			}
		}()
	}

	cancelChan := make(chan os.Signal)
	signal.Notify(cancelChan, os.Interrupt, syscall.SIGTERM)

	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case <-cancelChan:
				log.Println("Cancelling sending data")
				cancel()
				return
			case <-doneChan:
				return
			}
		}
	}()

	log.Println("Sending data...")
	wg.Wait()
	log.Println("Sending is finished")
	close(doneChan)
}
