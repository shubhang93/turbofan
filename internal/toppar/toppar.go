package toppar

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TopPart struct {
	Topic     string
	Partition int32
}

func (tp TopPart) MarshalText() (text []byte, err error) {
	return []byte(tp.String()), nil
}

type OffsetRange struct {
	Start int64
	End   int64
}

func (tp TopPart) String() string {
	return fmt.Sprintf("%s#%d", tp.Topic, tp.Partition)
}

func KafkaTopicPartToTopicPart(ktp kafka.TopicPartition) TopPart {
	return TopPart{
		Topic:     *ktp.Topic,
		Partition: ktp.Partition,
	}
}

func TopicPartToKafkaTopicPart(tp TopPart) kafka.TopicPartition {
	return kafka.TopicPartition{Topic: &tp.Topic, Partition: tp.Partition}
}

func KafkaTopPartsToTopParts(ktps []kafka.TopicPartition) []TopPart {
	tps := make([]TopPart, len(ktps))

	for i, ktp := range ktps {
		tps[i] = KafkaTopicPartToTopicPart(ktp)
	}

	return tps
}

func MakePartitionMap(messages []*kafka.Message) map[TopPart][]*kafka.Message {
	result := make(map[TopPart][]*kafka.Message, 128)
	for _, msg := range messages {
		tp := TopPart{
			Topic:     *msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
		}
		result[tp] = append(result[tp], msg)
	}

	return result
}
