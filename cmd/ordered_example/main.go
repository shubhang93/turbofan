package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/turbofan"
	"github.com/shubhang93/turbofan/internal/kafcon"
	"log"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	messageIn := make(chan []*kafka.Message)

	tbf := turbofan.New(kafcon.Config{
		BootstrapServers: []string{"localhost:9092"},
		CommitIntervalMS: 5000,
		ConsumerGroupID:  "turbofan_001",
		MessageBatchSize: 1000,
		LogLevels:        []string{"consumer"},
	}, messageIn)

	workersDone := make(chan struct{})

	turbofan.ProcessOrdered(ctx, messageIn, func(m *kafka.Message) {
		time.Sleep(100 * time.Millisecond)
		_ = tbf.ACK(m)
	})

	if err := tbf.Consume(ctx, []string{"topic1"}); err != nil {
		log.Println("error starting consumer:", err)
	}

	<-workersDone
}
