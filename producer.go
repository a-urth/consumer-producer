package main

import (
	"context"
	"encoding/csv"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

type Producer struct {
	Limit          int
	AmqpConnection *amqp.Connection
	AmqpChannel    *amqp.Channel
	RabbitURL      string
	Queue          string
}

func NewProducer(limit int, rabbitURL string, queue string) *Producer {
	return &Producer{Limit: limit, RabbitURL: rabbitURL, Queue: queue}
}

func (p Producer) Send(record []string) {
	return
}

func (p Producer) Close() {
	if p.AmqpChannel != nil {
		err := p.AmqpChannel.Close()
		check(err)
	}

	if p.AmqpConnection != nil {
		err := p.AmqpConnection.Close()
		check(err)
	}
}

func (p *Producer) initRabbit() {
	conn, err := amqp.Dial(p.RabbitURL)
	check(err)
	p.AmqpConnection = conn

	ch, err := conn.Channel()
	check(err)
	p.AmqpChannel = ch

	_, err = p.AmqpChannel.QueueDeclare(
		p.Queue,
		true,
		false,
		true,
		true,
		nil,
	)
	check(err)
}

func (p Producer) Run() {
	defer p.Close()

	(&p).initRabbit()

	filePath := flag.Arg(0)

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	check(err)

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	check(err)

	ctx, cancel := context.WithCancel(context.Background())
	recordsChan := make(chan []string, p.Limit)
	var wg sync.WaitGroup

	for i := 0; i < p.Limit; i++ {
		go func(ctx context.Context, recordsChan chan []string) {
			wg.Add(1)
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			case record := <-recordsChan:
				p.Send(record)
			}
		}(ctx, recordsChan)
	}

	for _, record := range records {
		recordsChan <- record
	}

	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, os.Interrupt, syscall.SIGTERM)

	doneChan := make(chan struct{})

	go func() {
		select {
		case <-cancelChan:
			cancel()
			os.Exit(1)
		case <-doneChan:
			return
		}
	}()

	wg.Wait()
	doneChan <- struct{}{}
}
