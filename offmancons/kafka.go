package offmancons

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

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
