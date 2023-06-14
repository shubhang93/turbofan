package offman

import (
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/turbofan/internal/toppar"
)

const defaultTrackerSize = 256

type TrackProgress struct {
	Message *kafka.Message
	toppar.TopPart
	Committed bool
}

type Manager struct {
	tracks              map[toppar.TopPart]*OffsetTrack
	mu                  sync.Mutex
	committableMessages map[toppar.TopPart]TrackProgress
	tpool               *TrackPool
}

func New(batchSize int) *Manager {
	return &Manager{
		tracks:              make(map[toppar.TopPart]*OffsetTrack, defaultTrackerSize),
		committableMessages: make(map[toppar.TopPart]TrackProgress, defaultTrackerSize),
		mu:                  sync.Mutex{},
		tpool:               NewTrackPool(batchSize),
	}
}

func (m *Manager) LoadPartitions(parts map[toppar.TopPart][]*kafka.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for tp, messages := range parts {
		track := m.tpool.Get()
		track.Load(messages)
		m.tracks[tp] = track
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
				TopPart:   tp,
				Committed: committed,
				Message:   message,
			}
		}
	}

	return m.committableMessages
}

func (m *Manager) ClearPartitions(topicParts []toppar.TopPart) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tp := range topicParts {
		track := m.tracks[tp]
		if track == nil {
			continue
		}
		m.tpool.Put(track)
		delete(m.tracks, tp)
		delete(m.committableMessages, tp)
	}
}

func (m *Manager) CommitSnapshot() map[string]int64 {
	// not a thread safe method
	// should be used for consumer shutdown purposes only
	snapshot := make(map[string]int64, len(m.tracks))

	for tp, track := range m.tracks {
		tpSerialized := fmt.Sprintf("%s", tp)
		snapshot[tpSerialized] = track.LastCommittedOffset()
	}
	return snapshot
}
