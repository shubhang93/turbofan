package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/relcon/consumer"
	"sync"
	"time"

	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	messageIn := make(chan []*kafka.Message)

	cons := consumer.New(consumer.Config{
		BootstrapServers: []string{"localhost:9092"},
		CommitIntervalMS: 5000,
		ConsumerGroupID:  "relcon_001",
		MessageBatchSize: 1000,
		LogLevels:        []string{"consumer"},
	}, messageIn)

	workersDone := make(chan struct{})

	// process each batch in its own goroutine, similar to
	// partition ordered processing using kafka consumer threads
	go func() {
		defer close(workersDone)

		var wg sync.WaitGroup
		for batch := range messageIn {
			wg.Add(1)
			go func(msgBatch []*kafka.Message) {
				for _, msg := range msgBatch {
					log.Printf("[process-func]:processing message {Part=%d,Topic=%s}", msg.TopicPartition.Partition, *msg.TopicPartition.Topic)
					time.Sleep(1000 * time.Millisecond)
					if err := cons.Ack(msg); err != nil {
						log.Println("error ACK-ing")
					}
				}
				defer wg.Done()
			}(batch)
		}

		wg.Wait()
	}()

	if err := cons.Consume(ctx, []string{"driver-location-ping-3"}); err != nil {
		log.Println("error starting consumer:", err)
	}

	<-workersDone
}
