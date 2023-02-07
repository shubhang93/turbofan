package offman

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/shubhang93/relcon/internal/toppar"
)

const defaultTrackerSize = 256

type TrackProgress struct {
	Message *kafka.Message
	toppar.TopPart
	Committed       bool
	PercentageAcked int64
}

type Manager struct {
	tracks              map[toppar.TopPart]*OffsetTrack `json:"tracks"`
	mu                  sync.Mutex
	committableMessages map[toppar.TopPart]TrackProgress
}

func New() *Manager {
	return &Manager{
		tracks:              make(map[toppar.TopPart]*OffsetTrack, defaultTrackerSize),
		mu:                  sync.Mutex{},
		committableMessages: make(map[toppar.TopPart]TrackProgress, defaultTrackerSize),
	}
}

func (m *Manager) LoadPartitions(parts map[toppar.TopPart][]*kafka.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for tp, messages := range parts {
		m.tracks[tp] = NewTrack(messages)
	}
}

func (m *Manager) Ack(tp toppar.TopPart, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offsetTrack, ok := m.tracks[tp]; ok {
		return offsetTrack.UpdateStatus(offset, StatusAck)
	}
	return nil
}

func (m *Manager) Nack(tp toppar.TopPart, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offsetTrack, ok := m.tracks[tp]; ok {
		return offsetTrack.UpdateStatus(offset, StatusNack)
	}
	return nil
}

func (m *Manager) MarkCommitCheckpoint(tp toppar.TopPart, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offsetTrack, ok := m.tracks[tp]; ok {
		// we are decrementing the offset by 1 because all stored offsets on kafka consumer are offset+1
		return offsetTrack.UpdateStatus(offset-1, StatusCommitted)
	}

	return nil
}

func (m *Manager) CommittableMessages() map[toppar.TopPart]TrackProgress {

	m.mu.Lock()
	defer m.mu.Unlock()
	for tp, offsetTrack := range m.tracks {
		message, ok := offsetTrack.CommittableMessage()
		if ok {
			committed := offsetTrack.Committed()
			m.committableMessages[tp] = TrackProgress{
				TopPart:         tp,
				Committed:       committed,
				Message:         message,
				PercentageAcked: int64(message.TopicPartition.Offset) / (offsetTrack.End),
			}
		}
	}

	return m.committableMessages
}

func (m *Manager) ClearPartitions(topicParts []toppar.TopPart) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tp := range topicParts {
		delete(m.tracks, tp)
		delete(m.committableMessages, tp)
	}
}
