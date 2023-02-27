package worker

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sync"
)

func ProcessOrdered(ctx context.Context, messageIn chan []*kafka.Message, callback func(m *kafka.Message)) {
	var wg sync.WaitGroup
	for messages := range messageIn {
		wg.Add(1)
		go func(msgs []*kafka.Message) {
			defer wg.Done()
			for i := range msgs {
				select {
				case <-ctx.Done():
					return
				default:
					callback(msgs[i])
				}
			}
		}(messages)
	}
	wg.Wait()
}
