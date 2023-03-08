package offman

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/turbofan/internal/toppar"
	"reflect"
	"testing"
)

func strPtr(s string) *string {
	return &s
}

func TestLoadPartitions(t *testing.T) {
	man := New(2)
	parts := map[toppar.TopPart][]*kafka.Message{
		toppar.TopPart{Partition: 0, Topic: "a"}: {
			{TopicPartition: kafka.TopicPartition{Offset: 200}},
			{TopicPartition: kafka.TopicPartition{Offset: 201}},
		},
		toppar.TopPart{Partition: 1, Topic: "b"}: {
			{TopicPartition: kafka.TopicPartition{Offset: 505}},
		},
	}

	offsetTrackOne := OffsetTrack{
		messages: map[int64]*MessageContainer{
			200: {
				Message:   &kafka.Message{TopicPartition: kafka.TopicPartition{Offset: 200}},
				ACKStatus: 0,
			},
			201: {
				Message:   &kafka.Message{TopicPartition: kafka.TopicPartition{Offset: 201}},
				ACKStatus: 0,
			},
		},
		commitCheckpoint: 0,
		needleOffset:     0,
		Start:            200,
		End:              201,
	}

	offsetTrackTwo := OffsetTrack{
		messages: map[int64]*MessageContainer{
			505: {
				Message: &kafka.Message{TopicPartition: kafka.TopicPartition{Offset: 505}},
			},
		},
		commitCheckpoint: 0,
		needleOffset:     0,
		Start:            505,
		End:              505,
	}

	tracks := map[toppar.TopPart]*OffsetTrack{
		toppar.TopPart{Partition: 0, Topic: "a"}: &offsetTrackOne,
		toppar.TopPart{Topic: "b", Partition: 1}: &offsetTrackTwo,
	}
	man.LoadPartitions(parts)

	for tp, expectedTrack := range tracks {
		gotTrack, ok := man.tracks[tp]
		if !ok {
			t.Errorf("expected tp:%v not present\n", tp)
			return
		}
		if !reflect.DeepEqual(*expectedTrack, *gotTrack) {
			t.Errorf("expected track:%+v\n got track:%+v\n", *expectedTrack, *gotTrack)
		}
	}
}

func TestManager_MarkCommitCheckpoint(t *testing.T) {
	man := New(2)
	parts := map[toppar.TopPart][]*kafka.Message{
		toppar.TopPart{Partition: 0, Topic: "foo"}: {{TopicPartition: kafka.TopicPartition{
			Topic:     strPtr("foo"),
			Partition: 0,
			Offset:    101,
		}}},
	}
	man.LoadPartitions(parts)

	if err := man.MarkCommitCheckpoint(toppar.TopPart{Topic: "foo", Partition: 0}, 102); err != nil {
		t.Errorf("error marking commit checkpoint:%v\n", err)
		return
	}
}
