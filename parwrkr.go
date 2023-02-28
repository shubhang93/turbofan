package turbofan

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sync"
)

type Options struct {
	Concurrency int
}

type Opts func(*Options)

func WithConcurrency(concurrency int) Opts {
	return func(options *Options) {
		if concurrency >= 1 {
			options.Concurrency = concurrency
		}
	}
}

func processParallel(ctx context.Context, msgs []*kafka.Message, concurrency int, cb func(m *kafka.Message)) {
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

func ProcessParallel(ctx context.Context, messageIn chan []*kafka.Message, callback func(m *kafka.Message), opts ...Opts) {

	options := &Options{Concurrency: 10}
	for _, opt := range opts {
		opt(options)
	}

	var wg sync.WaitGroup
	for messages := range messageIn {
		wg.Add(1)
		go func(msgs []*kafka.Message) {
			defer wg.Done()
			processParallel(ctx, msgs, options.Concurrency, callback)
		}(messages)
	}
	wg.Wait()
}
