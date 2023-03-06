package offman

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"reflect"
	"testing"
)

func TestTrackPool_Get(t *testing.T) {
	tp := NewTrackPool(100)
	ot := tp.Get()
	if cap(ot.order) != 100 {
		t.Errorf("expected %d got %d\n", 100, cap(ot.order))
	}
}

func TestTrackPool_Put(t *testing.T) {
	tp := NewTrackPool(2)
	ot := tp.Get()
	messages := []*kafka.Message{
		{TopicPartition: kafka.TopicPartition{Partition: 1, Offset: 10}},
		{TopicPartition: kafka.TopicPartition{Partition: 1, Offset: 11}}}
	ot.Load(messages)
	if len(ot.messages) != len(messages) {
		t.Errorf("expected track to contain %d message got %d messages\n", len(ot.messages), len(messages))
		return
	}
	ot.Reset()
	if len(ot.messages) != 0 {
		t.Errorf("expected messages to be 0 after reset, got %d\n", len(ot.messages))
		return
	}

	newTrack := &OffsetTrack{
		messages: make(map[int64]*MessageContainer, 2),
		order:    make([]int64, 2),
	}
	if !reflect.DeepEqual(ot, newTrack) {
		t.Errorf("expected %+v got %+v\n", newTrack, ot)
	}

}
