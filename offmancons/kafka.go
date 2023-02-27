package offmancons

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer interface {
	Poll(int) kafka.Event
	Resume([]kafka.TopicPartition) error
	Pause([]kafka.TopicPartition) error
	SubscribeTopics([]string, kafka.RebalanceCb) error
	StoreMessage(message *kafka.Message) ([]kafka.TopicPartition, error)
	StoreOffsets([]kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Commit() ([]kafka.TopicPartition, error)
	String() string
	Close() error
}

type MockConsumer struct {
	PollFunc            func(int) kafka.Event
	ResumeFunc          func([]kafka.TopicPartition) error
	PauseFunc           func([]kafka.TopicPartition) error
	SubscribeTopicsFunc func([]string, kafka.RebalanceCb) error
	StoreMessageFunc    func(message *kafka.Message) ([]kafka.TopicPartition, error)
	StoreOffsetsFunc    func([]kafka.TopicPartition) ([]kafka.TopicPartition, error)
	CommitFunc          func() ([]kafka.TopicPartition, error)
	StringFunc          func() string
	CloseFunc           func() error
}

func (m MockConsumer) Poll(i int) kafka.Event {
	if m.PollFunc == nil {
		return &kafka.Message{}
	}
	return m.PollFunc(i)
}

func (m MockConsumer) Resume(partitions []kafka.TopicPartition) error {
	if m.ResumeFunc == nil {
		return nil
	}
	return m.ResumeFunc(partitions)
}

func (m MockConsumer) Pause(partitions []kafka.TopicPartition) error {
	if m.PauseFunc == nil {
		return nil
	}
	return m.PauseFunc(partitions)
}

func (m MockConsumer) SubscribeTopics(topics []string, cb kafka.RebalanceCb) error {
	if m.SubscribeTopicsFunc == nil {
		return nil
	}
	return m.SubscribeTopicsFunc(topics, cb)
}

func (m MockConsumer) StoreMessage(message *kafka.Message) ([]kafka.TopicPartition, error) {
	if m.StoreMessageFunc == nil {
		return []kafka.TopicPartition{}, nil
	}
	return m.StoreMessageFunc(message)
}

func (m MockConsumer) StoreOffsets(partitions []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	if m.StoreOffsetsFunc == nil {
		return []kafka.TopicPartition{}, nil
	}
	return m.StoreOffsetsFunc(partitions)
}

func (m MockConsumer) Commit() ([]kafka.TopicPartition, error) {

	if m.CommitFunc == nil {
		return []kafka.TopicPartition{}, nil
	}

	return m.CommitFunc()
}

func (m MockConsumer) String() string {
	if m.StringFunc == nil {
		return "test-consumer"
	}
	return m.StringFunc()
}

func (m MockConsumer) Close() error {
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}
