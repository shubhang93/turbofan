package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/turbofan/offmancons"
	"github.com/shubhang93/turbofan/worker"
	"log"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	messageIn := make(chan []*kafka.Message)

	cons := offmancons.New(offmancons.Config{
		BootstrapServers: []string{"localhost:9092"},
		CommitIntervalMS: 5000,
		ConsumerGroupID:  "turbofan_001",
		MessageBatchSize: 1000,
		LogLevels:        []string{"consumer"},
	}, messageIn)

	workersDone := make(chan struct{})

	worker.ProcessOrdered(ctx, messageIn, func(m *kafka.Message) {
		time.Sleep(100 * time.Millisecond)
		_ = cons.ACK(m)
	})

	if err := cons.Consume(ctx, []string{"topic1"}); err != nil {
		log.Println("error starting consumer:", err)
	}

	<-workersDone
}
