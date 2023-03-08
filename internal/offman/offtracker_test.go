package offman

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func toPtrStr(v string) *string {
	return &v
}
func TestCommittableMessageACK(t *testing.T) {

	type test struct {
		Name      string
		BatchSize int
	}

	tests := []test{{
		Name:      "Messages are equal to the batch size",
		BatchSize: 5,
	},
		{
			Name:      "Messages are less than the batch size",
			BatchSize: 10,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			messages := []*kafka.Message{
				{TopicPartition: kafka.TopicPartition{Offset: 1}},
				{TopicPartition: kafka.TopicPartition{Offset: 2}},
				{TopicPartition: kafka.TopicPartition{Offset: 3}},
				{TopicPartition: kafka.TopicPartition{Offset: 4}},
				{TopicPartition: kafka.TopicPartition{Offset: 5}},
			}

			track := NewTrackPool(test.BatchSize).Get()
			track.Load(messages)

			// first offset is not ACKED
			msg, ok := track.CommittableMessage()
			if ok {
				t.Errorf("expected ok to be false")
				return
			}

			// Middle offset is not ACKED
			_ = track.UpdateStatus(1, StatusAck)
			_ = track.UpdateStatus(2, StatusAck)

			msg, _ = track.CommittableMessage()
			committableOffset := int64(2)

			if int64(msg.TopicPartition.Offset) != committableOffset {
				t.Errorf("expected offset %d got offset %d\n", committableOffset, msg.TopicPartition.Offset)
				return
			}

			//All offsets are ACKED
			_ = track.UpdateStatus(3, StatusAck)
			_ = track.UpdateStatus(4, StatusAck)
			_ = track.UpdateStatus(5, StatusAck)

			msg, _ = track.CommittableMessage()
			committableOffset = int64(5)

			if int64(msg.TopicPartition.Offset) != committableOffset {
				t.Errorf("expected offset %d got offset %d\n", committableOffset, msg.TopicPartition.Offset)
				return
			}

			if !track.Finished() {
				t.Errorf("Expected track to be finished")
				return
			}

			err := track.UpdateStatus(6, StatusAck)
			if err == nil {
				t.Errorf("expected an out of range offset error")
				return
			}
			t.Logf("%v\n", err)
		})
	}

}

func TestOffsetTrack_Reset(t *testing.T) {
	want := &OffsetTrack{
		messages: make(map[int64]*MessageContainer, 2),
	}
	got := NewTrackPool(2).Get()
	got.Load([]*kafka.Message{
		{
			TopicPartition: kafka.TopicPartition{Offset: 100, Topic: toPtrStr("foo"), Partition: 1},
		},
		{
			TopicPartition: kafka.TopicPartition{Offset: 101, Topic: toPtrStr("foo"), Partition: 1},
		}})

	got.Reset()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %+v\n got %+v\n", want, got)
	}
}
