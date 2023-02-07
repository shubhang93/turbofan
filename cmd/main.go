package main

import (
	"log"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/net/context"
	"source.golabs.io/engineering-platforms/ziggurat/kafka-consumer-proxy-spike/consumer"
)

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	messageIn := make(chan []*kafka.Message)

	cons := consumer.New(consumer.Config{
		BootstrapServers: []string{"g-gojek-id-mainstream.golabs.io:6668"},
		CommitIntervalMS: 5000,
		ConsumerGroupID:  "relcon_001",
		MessageBatchSize: 1000,
		LogLevels:        []string{"consumer"},
	}, messageIn)

	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		RunWorkers(ctx, messageIn, func(m *kafka.Message) {
			sleepTime := rand.Intn(10)
			log.Printf("[Worker]: processing message %v\n", m.TopicPartition)
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
			if err := cons.Ack(m); err != nil {
				log.Println("ACK error:", err)
			}
		})
	}()

	if err := cons.Consume(ctx, []string{"driver-location-ping-3"}); err != nil {
		log.Println("error starting consumer:", err)
	}

	<-workersDone

}
