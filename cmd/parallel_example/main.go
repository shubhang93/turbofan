package main

import (
	"github.com/shubhang93/relcon/worker"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/relcon/offmancons"
	"golang.org/x/net/context"
)

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	messageIn := make(chan []*kafka.Message)

	cons := offmancons.New(offmancons.Config{
		BootstrapServers: []string{"localhost:9092"},
		CommitIntervalMS: 5000,
		ConsumerGroupID:  "relcon_001",
		MessageBatchSize: 1000,
		LogLevels:        []string{"consumer"},
	}, messageIn)

	workersDone := make(chan struct{})

	// process each message concurrently with an upper limit of 10 goroutines
	go func() {
		defer close(workersDone)
		worker.ProcessParallel(ctx, messageIn, func(m *kafka.Message) {
			log.Printf("[Worker]: processing message %v\n", m.TopicPartition)
			time.Sleep(1000 * time.Millisecond)
			if err := cons.Ack(m); err != nil {
				log.Println("ACK error:", err)
			}
		})
	}()

	if err := cons.Consume(ctx, []string{"topic1"}); err != nil {
		log.Println("error starting consumer:", err)
	}

	<-workersDone

}
