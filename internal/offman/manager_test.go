package offman

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/turbofan/internal/toppar"
	"reflect"
	"sync"
	"testing"
)

func strPtr(s string) *string {
	return &s
}

func makeRecords(start, end int64, topic string, partition int32) []*kafka.Message {
	var records []*kafka.Message
	for i := start; i <= end; i++ {
		records = append(records, &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     strPtr(topic),
				Partition: partition,
				Offset:    kafka.Offset(i)},
		})
	}
	return records
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

func TestManager_ConcurrentAck(t *testing.T) {
	man := New(10)
	parts := map[toppar.TopPart][]*kafka.Message{
		toppar.TopPart{Partition: 1, Topic: "foo"}: makeRecords(100, 110, "foo", 1),
		toppar.TopPart{Partition: 2, Topic: "foo"}: makeRecords(200, 210, "foo", 2),
	}

	var messages []*kafka.Message

	for _, records := range parts {
		messages = append(messages, records...)
	}

	in := make(chan *kafka.Message)
	go func() {
		for i := range messages {
			in <- messages[i]
		}
		close(in)
	}()

	doNotAck := map[int64]bool{
		103: true,
		105: true,
		209: true,
	}
	man.LoadPartitions(parts)

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range in {
				offset := msg.TopicPartition.Offset
				if doNotAck[int64(offset)] {
					continue
				}
				tp := toppar.TopPart{
					Topic:     *msg.TopicPartition.Topic,
					Partition: msg.TopicPartition.Partition,
				}
				_ = man.Ack(tp, int64(offset))
			}
		}()
	}

	wg.Wait()

	expectedCommittableOffsets := map[toppar.TopPart]int64{
		toppar.TopPart{Partition: 1, Topic: "foo"}: 102,
		toppar.TopPart{Partition: 2, Topic: "foo"}: 208,
	}

	committable := man.CommittableMessages()

	for _, progress := range committable {
		offset := int64(progress.Message.TopicPartition.Offset)
		expected := expectedCommittableOffsets[progress.TopPart]
		if expected != offset {
			t.Errorf("expected: %v got %v\n", expected, offset)
		}
	}
}
