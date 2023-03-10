//go:build ignore

package main

import (
	"github.com/shubhang93/turbofan"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"golang.org/x/net/context"
)

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	messageIn := make(chan []*kafka.Message)

	cons := turbofan.New(turbofan.Config{
		BootstrapServers: []string{"localhost:9092"},
		CommitIntervalMS: 5000,
		ConsumerGroupID:  "turbofan_001",
		MessageBatchSize: 1000,
		LogLevels:        []string{"consumer"},
	}, messageIn)

	workersDone := make(chan struct{})

	// process each message concurrently with an upper limit of 10 goroutines
	go func() {
		defer close(workersDone)
		turbofan.ProcessParallel(ctx, messageIn, func(m *kafka.Message) {
			log.Printf("[Worker]: processing message %v\n", m.TopicPartition)
			time.Sleep(1000 * time.Millisecond)
			if err := cons.ACK(m); err != nil {
				log.Println("ACK error:", err)
			}
		})
	}()

	if err := cons.Consume(ctx, []string{"topic1"}); err != nil {
		log.Println("error starting consumer:", err)
	}

	<-workersDone

}
