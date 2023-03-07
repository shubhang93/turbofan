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
	needleOffset     int64
	Start            int64
	End              int64
}

func (t *OffsetTrack) Load(msgs []*kafka.Message) {
	t.Start = int64(msgs[0].TopicPartition.Offset)
	t.End = int64(msgs[len(msgs)-1].TopicPartition.Offset)

	for _, msg := range msgs {
		offset := int64(msg.TopicPartition.Offset)
		//t.order[i] = offset
		t.messages[offset] = &MessageContainer{Message: msg}
	}
}

func (t *OffsetTrack) UpdateStatus(offset int64, status StatusACK) error {

	mc, ok := t.messages[offset]
	start, end := t.Start, t.End

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
	offset := t.needleOffset

	for offset <= t.End {

		// for first offset
		if offset == 0 {
			offset = t.Start
		}

		msg := t.messages[offset]

		if msg.ACKStatus == StatusNack {
			break
		}
		t.needleOffset = offset
		offset++
	}

	container, ok := t.messages[t.needleOffset]

	if !ok {
		return nil, ok
	}

	return container.Message, ok

}

func (t *OffsetTrack) Finished() bool {
	return t.needleOffset == t.End
}

func (t *OffsetTrack) Committed() bool {
	return t.messages[t.needleOffset].ACKStatus == StatusCommitted
}

func (t *OffsetTrack) LastCommittedOffset() int64 {
	return t.commitCheckpoint
}

func (t *OffsetTrack) Reset() {

	t.Start = 0
	t.End = 0
	t.needleOffset = 0
	t.commitCheckpoint = 0

	for off := range t.messages {
		delete(t.messages, off)
	}

}
