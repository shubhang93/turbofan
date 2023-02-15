package main

import (
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"golang.org/x/net/context"
)

func process(ctx context.Context, msgs []*kafka.Message, concurrency int, cb func(m *kafka.Message)) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, msg := range msgs {
		sem <- struct{}{}
		wg.Add(1)
		go func(m *kafka.Message) {
			defer func() {
				<-sem
				wg.Done()
			}()

			select {
			case <-ctx.Done():
				return
			default:
				cb(m)
			}
		}(msg)
	}
	wg.Wait()
}

func RunWorkers(ctx context.Context, messageIn chan []*kafka.Message, callback func(m *kafka.Message)) {
	var wg sync.WaitGroup
	for messages := range messageIn {
		log.Println("Received payload of length", len(messages))
		wg.Add(1)
		go func(msgs []*kafka.Message) {
			defer wg.Done()
			process(ctx, msgs, 10, callback)
		}(messages)
	}
	wg.Wait()
}
