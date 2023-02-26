package offmancons

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/relcon/internal/offman"
	"reflect"
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

func makeTopicPartition(topic string, part int32, offset kafka.Offset) kafka.TopicPartition {
	return kafka.TopicPartition{Topic: toPtrStr(topic), Partition: part, Offset: offset}
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

func TestOffManConsumer_PartitionEOF(t *testing.T) {

	expectedPartition := kafka.TopicPartition{
		Topic:     toPtrStr("foo"),
		Partition: 1,
		Offset:    200,
	}

	mock := MockConsumer{
		StoreOffsetsFunc: func(partitions []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
			gotPartition := partitions[0]
			gotPartition.Offset = 200
			if !reflect.DeepEqual(gotPartition, expectedPartition) {
				t.Errorf("Expected %+v Got %+v\n", expectedPartition, gotPartition)
			}
			return []kafka.TopicPartition{}, nil
		},
		PollFunc: func(i int) kafka.Event {
			time.Sleep(50 * time.Millisecond)
			return kafka.PartitionEOF(expectedPartition)
		},
	}

	omc := OffManConsumer{batchSize: 100, kafCon: &mock}
	_, _ = omc.pollBatch(context.Background(), 100)

}

func TestOffManConsumer_Consume_runLoop(t *testing.T) {
	expectedTopicPartition := kafka.TopicPartition{
		Topic:     toPtrStr("foo"),
		Partition: 1,
	}

	gotCallCount := 0
	expectedCallCount := 2

	mock := MockConsumer{
		PollFunc: func() func(i int) kafka.Event {
			offset := 1
			return func(i int) kafka.Event {
				if offset > 10 {
					return nil
				}
				time.Sleep(10 * time.Millisecond)
				defer func() { offset++ }()
				return &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     expectedTopicPartition.Topic,
						Partition: expectedTopicPartition.Partition,
						Offset:    kafka.Offset(offset),
					},
				}
			}
		}(),
		ResumeFunc: func(partitions []kafka.TopicPartition) error {
			defer func() { gotCallCount++ }()
			part := partitions[0]
			if !reflect.DeepEqual(part, expectedTopicPartition) {
				return fmt.Errorf("[resume]: expected partition %v got partition %v\n", part, expectedTopicPartition)
			}
			return nil
		},
		PauseFunc: func(partitions []kafka.TopicPartition) error {
			defer func() { gotCallCount++ }()
			part := partitions[0]
			if !reflect.DeepEqual(part, expectedTopicPartition) {
				return fmt.Errorf("[pause]:expected part:%v got part:%v\n", expectedTopicPartition, part)
			}
			return nil
		},
		SubscribeTopicsFunc: func(strings []string, cb kafka.RebalanceCb) error {
			return nil
		},
		StoreMessageFunc: func(message *kafka.Message) ([]kafka.TopicPartition, error) {
			expectedReceive := makeTopicPartition("foo", 1, 10)
			if !reflect.DeepEqual(message.TopicPartition, expectedReceive) {
				return nil, fmt.Errorf("[storeMessage]:expected part:%v got part:%v\n", expectedTopicPartition, message.TopicPartition)
			}
			return []kafka.TopicPartition{expectedTopicPartition}, nil
		},
		CommitFunc: func() ([]kafka.TopicPartition, error) {
			topicPartition := makeTopicPartition("foo", 1, 11)
			return []kafka.TopicPartition{topicPartition}, nil
		},
		CloseFunc: func() error {
			return nil
		},
	}

	sendChan := make(chan []*kafka.Message)
	omc := OffManConsumer{
		kafCon:           &mock,
		offMan:           offman.New(),
		commitIntervalMS: 500,
		lastCommit:       time.Now(),
		batchSize:        100,
		sendChan:         sendChan,
	}

	wait := make(chan struct{})
	go func() {
		for batch := range sendChan {
			t.Logf("received batch of len %d\n", len(batch))
			for _, msg := range batch {
				t.Logf("processing %v\n", msg)
				time.Sleep(5 * time.Millisecond)
				_ = omc.Ack(msg)
			}
		}
		close(wait)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	err := omc.Consume(ctx, []string{"foo"})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("%v\n", err)
	}
	if gotCallCount != expectedCallCount {
		t.Errorf("expected call count %d got %d\n", expectedCallCount, gotCallCount)
	}

	<-wait
}
