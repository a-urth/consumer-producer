package main

import (
	"log"

	"github.com/streadway/amqp"
)

type Amqp struct {
	AmqpConnection *amqp.Connection
	AmqpChannel    *amqp.Channel
	RabbitURL      string
	Queue          string
}

func (a *Amqp) InitRabbit() {
	conn, err := amqp.Dial(a.RabbitURL)
	check(err)
	a.AmqpConnection = conn

	ch, err := conn.Channel()
	check(err)
	a.AmqpChannel = ch

	_, err = a.AmqpChannel.QueueDeclare(
		a.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	check(err)

	log.Println("RabbitMQ connection established")
}

func (a Amqp) Close() {
	if a.AmqpChannel != nil {
		err := a.AmqpChannel.Close()
		check(err)
	}

	if a.AmqpConnection != nil {
		err := a.AmqpConnection.Close()
		check(err)
	}

	log.Println("RabbitMQ connection closed")
}
