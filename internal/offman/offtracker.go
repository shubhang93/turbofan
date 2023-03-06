package offman

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MessageContainer struct {
	Message   *kafka.Message
	ACKStatus StatusACK
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
		order:    make([]int64, len(messages)),
		End:      int64(messages[len(messages)-1].TopicPartition.Offset),
		Start:    int64(messages[0].TopicPartition.Offset),
	}

	for i, msg := range messages {
		offset := int64(msg.TopicPartition.Offset)
		t.order[i] = offset
		t.messages[offset] = &MessageContainer{Message: msg}
	}

	return &t
}

func (t *OffsetTrack) Load(msgs []*kafka.Message) {
	t.Start = int64(msgs[0].TopicPartition.Offset)
	t.End = int64(msgs[len(msgs)-1].TopicPartition.Offset)

	for _, msg := range msgs {
		offset := int64(msg.TopicPartition.Offset)
		t.order = append(t.order, offset)
		t.messages[offset] = &MessageContainer{Message: msg}
	}
}

func (t *OffsetTrack) UpdateStatus(offset int64, status StatusACK) error {

	mc, ok := t.messages[offset]
	start, end := t.order[0], t.order[len(t.order)-1]

	if !ok {
		return fmt.Errorf("{offset=%d status=%s base=%d end=%d} offset update is out of range", offset, status, start, end)
	}

	mc.ACKStatus = status
	if status == StatusCommitted {
		t.commitCheckpoint = offset
	}
	return nil
}

func (t *OffsetTrack) CommittableMessage() (*kafka.Message, bool) {

	for i := t.needle; i < len(t.order); i++ {
		offset := t.order[i]
		msg := t.messages[offset]
		if msg.ACKStatus == StatusNack {
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
	return t.messages[lastOffset].ACKStatus == StatusCommitted
}

func (t *OffsetTrack) LastCommittedOffset() int64 {
	return t.commitCheckpoint
}

func (t *OffsetTrack) Reset() {

	t.Start = 0
	t.End = 0
	t.needle = 0
	t.needleOffset = 0
	t.commitCheckpoint = 0

	for off := range t.messages {
		delete(t.messages, off)
	}

	t.order = t.order[:0]
}
