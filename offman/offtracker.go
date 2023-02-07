package offman

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageContainer struct {
	Message *kafka.Message
	Ack     AckStatus
}

type OffsetTrack struct {
	messages         map[int64]*MessageContainer
	commitCheckpoint int64
	order            []int64
	needle           int
	needleOffset     int64
	Start            int64
	End              int64
}

func NewTrack(messages []*kafka.Message) *OffsetTrack {

	t := OffsetTrack{
		messages: make(map[int64]*MessageContainer, len(messages)),
		End:      int64(messages[len(messages)-1].TopicPartition.Offset),
		Start:    int64(messages[0].TopicPartition.Offset),
	}
	for _, msg := range messages {
		offset := int64(msg.TopicPartition.Offset)
		t.order = append(t.order, offset)
		t.messages[offset] = &MessageContainer{Message: msg}
	}

	return &t
}

func (t *OffsetTrack) UpdateStatus(offset int64, status AckStatus) error {

	mc, ok := t.messages[offset]
	start, end := t.order[0], t.order[len(t.order)-1]

	if !ok {
		return fmt.Errorf("{offset=%d status=%s base=%d end=%d} offset update is out of range", offset, status, start, end)
	}

	mc.Ack = status
	if status == StatusCommitted {
		t.commitCheckpoint = offset
	}
	return nil
}

func (t *OffsetTrack) CommittableMessage() (*kafka.Message, bool) {

	for i := t.needle; i < len(t.order); i++ {
		offset := t.order[i]
		msg := t.messages[offset]
		if msg.Ack == StatusNack {
			break
		}
		t.needle = i
		t.needleOffset = offset
	}

	container, ok := t.messages[t.needleOffset]

	if !ok {
		return nil, ok
	}

	return container.Message, ok

}

func (t *OffsetTrack) Finished() bool {
	return t.needle == len(t.order)-1
}

func (t *OffsetTrack) Committed() bool {
	lastOffset := t.order[len(t.order)-1]
	return t.messages[lastOffset].Ack == StatusCommitted
}

func (t *OffsetTrack) CommitCheckpoint() int64 {
	return t.commitCheckpoint
}
