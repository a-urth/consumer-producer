package main

import (
	"flag"
	"log"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Service interface {
	Run()
}

type User struct {
	Email    string `json:"email"`
	FullName string `json:"fullname"`
}

func main() {
	isConsumer := flag.Bool("consumer", false, "if consumer should be started")
	isProducer := flag.Bool("producer", false, "if producer should be started")

	limit := flag.Int("limit", 10, "limit number of concurrent requests")
	rabbitURL := flag.String(
		"consumer url",
		"amqp://localhost:5672",
		"url for consumer",
	)
	queue := flag.String("queue", "simple", "Exchange queue name")
	db := flag.String(
		"db",
		"postgresql://postgres:postgres@127.0.0.1:5432/simple",
		"Db url",
	)

	flag.Parse()

	if *isConsumer == *isProducer {
		log.Fatalln("Only one consumer or producer should be started")
	}

	var service Service
	if *isConsumer {
		service = NewConsumer(*limit, *rabbitURL, *queue, *db)
	} else {
		service = NewProducer(*limit, *rabbitURL, *queue)
	}

	service.Run()
}
