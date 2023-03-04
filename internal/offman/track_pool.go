package offman

import "sync"

type TrackPool struct {
	pool *sync.Pool
}

func NewTrackPool(batchSize int) *TrackPool {
	pool := sync.Pool{
		New: func() any {
			return &OffsetTrack{
				messages: make(map[int64]*MessageContainer, batchSize),
				order:    make([]int64, batchSize),
			}
		},
	}
	return &TrackPool{pool: &pool}
}

func (tp *TrackPool) Get() *OffsetTrack {
	return tp.pool.Get().(*OffsetTrack)
}

func (tp *TrackPool) Put(ot *OffsetTrack) {
	ot.Reset()
	tp.pool.Put(ot)
}
