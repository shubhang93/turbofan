package offmancons

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"testing"
	"time"
)

func toPtrStr(s string) *string {
	return &s
}

var record = &kafka.Message{
	TopicPartition: kafka.TopicPartition{
		Partition: 0,
		Offset:    1,
		Topic:     toPtrStr("foo"),
	},
}

func makeRecords(count int) []*kafka.Message {
	records := make([]*kafka.Message, count)
	for i := 0; i < count; i++ {
		records[i] = record
	}
	return records
}

func TestOffManConsumer_pollBatch(t *testing.T) {
	type pollTest struct {
		Name         string
		MockConsumer func() *MockConsumer
		Want         []*kafka.Message
		WantError    bool
		CtxFunc      func() (context.Context, context.CancelFunc)
	}

	tests := []pollTest{{
		Name: "Poll returns an event of type *kafka.Message",
		MockConsumer: func() *MockConsumer {
			return &MockConsumer{
				PollFunc: func(_ int) kafka.Event {
					time.Sleep(10 * time.Millisecond)
					return record
				},
			}
		},
		Want:      makeRecords(10),
		WantError: false,
	},
		{
			Name: "Poll returns one non fatal error event and message events",
			MockConsumer: func() *MockConsumer {
				return &MockConsumer{
					PollFunc: func() func(int) kafka.Event {
						counter := 0
						return func(_ int) kafka.Event {
							defer func() { counter++ }()

							if counter > 9 {
								return nil
							}

							if counter == 2 {
								return kafka.NewError(kafka.ErrIntr, "", false)
							}
							return &kafka.Message{}
						}
					}(),
				}
			},
			Want:      makeRecords(9),
			WantError: false,
		},
		{
			Name: "Poll returns a fatal error and message events",
			MockConsumer: func() *MockConsumer {
				return &MockConsumer{
					PollFunc: func() func(i int) kafka.Event {
						counter := 0
						return func(_ int) kafka.Event {
							defer func() { counter++ }()
							if counter == 2 {
								return kafka.NewError(kafka.ErrIntr, "", true)
							}
							return &kafka.Message{}
						}
					}(),
				}
			},
			Want:      makeRecords(0),
			WantError: true,
		},
		{
			Name: "Poll returns a max of batchSize number of messages",
			MockConsumer: func() *MockConsumer {
				return &MockConsumer{
					PollFunc: func(i int) kafka.Event {
						return &kafka.Message{}
					},
				}
			},
			Want:      makeRecords(100),
			WantError: false,
		},
		{
			Name: "Poll returns an error when context is cancelled",
			MockConsumer: func() *MockConsumer {
				return &MockConsumer{
					PollFunc: func(i int) kafka.Event {
						return &kafka.Message{}
					},
				}
			},
			WantError: true,
			CtxFunc: func() (context.Context, context.CancelFunc) {
				c, cancel := context.WithCancel(context.Background())
				cancel()
				return c, cancel
			},
		},
	}

	for _, testCase := range tests {

		t.Run(testCase.Name, func(t *testing.T) {
			mock := testCase.MockConsumer()
			omc := OffManConsumer{
				batchSize: 100,
				kafCon:    mock,
			}

			ctx, cancel := context.WithCancel(context.Background())
			if testCase.CtxFunc != nil {
				ctx, cancel = testCase.CtxFunc()
			}
			defer cancel()

			messages, err := omc.pollBatch(ctx, 100)
			if testCase.WantError && err == nil {
				t.Errorf("expected error but got nil\n")
				return
			}
			if len(messages) != len(testCase.Want) {
				t.Errorf("expected %d messages got %d messages\n", len(testCase.Want), len(messages))
			}
		})

	}

}
