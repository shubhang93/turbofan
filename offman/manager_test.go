package offman

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/relcon/internal/toppar"
)

func TestLoadPartitions(t *testing.T) {
	man := New()
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
				Message: &kafka.Message{TopicPartition: kafka.TopicPartition{Offset: 200}},
				Ack:     0,
			},
			201: {
				Message: &kafka.Message{TopicPartition: kafka.TopicPartition{Offset: 201}},
				Ack:     0,
			},
		},
		commitCheckpoint: 0,
		order:            []int64{200, 201},
		needle:           0,
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
		order:            []int64{505},
		needle:           0,
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
			t.Errorf("expected track:%v got track:%v\n", *expectedTrack, *gotTrack)
		}
	}

}
