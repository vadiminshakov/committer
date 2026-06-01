package viz

import (
	"sync"
	"time"

	"github.com/vadiminshakov/committer/tui/events"
)

type Collector struct {
	mu     sync.RWMutex
	events []EventDTO
	inner  events.Emitter
}

type EventDTO struct {
	Kind      uint8     `json:"kind"`
	KindName  string    `json:"kindName"`
	Timestamp time.Time `json:"timestamp"`
	Key       string    `json:"key,omitempty"`
	Height    uint64    `json:"height"`
	Cohort    string    `json:"cohort,omitempty"`
	Result    string    `json:"result,omitempty"`
	Message   string    `json:"message,omitempty"`
	Level     string    `json:"level,omitempty"`
}

func NewCollector(inner events.Emitter) *Collector {
	if inner == nil {
		inner = events.NoopEmitter{}
	}
	return &Collector{inner: inner}
}

func (c *Collector) Emit(ev events.Event) {
	c.inner.Emit(ev)

	dto := EventDTO{
		Kind:      uint8(ev.Kind),
		KindName:  kindName(ev.Kind),
		Timestamp: ev.Timestamp,
		Key:       ev.Key,
		Height:    ev.Height,
		Cohort:    ev.Cohort,
		Result:    ev.Result,
		Message:   ev.Message,
		Level:     ev.Level,
	}
	if dto.Timestamp.IsZero() {
		dto.Timestamp = time.Now()
	}

	c.mu.Lock()
	c.events = append(c.events, dto)
	c.mu.Unlock()
}

func (c *Collector) Events() []EventDTO {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]EventDTO, len(c.events))
	copy(out, c.events)
	return out
}

func kindName(k events.EventKind) string {
	switch k {
	case events.EvCoordPropose:
		return "CoordPropose"
	case events.EvCoordPrecommit:
		return "CoordPrecommit"
	case events.EvCoordCommit:
		return "CoordCommit"
	case events.EvCoordAbort:
		return "CoordAbort"
	case events.EvCoordHeightSync:
		return "CoordHeightSync"
	case events.EvCohortPropose:
		return "CohortPropose"
	case events.EvCohortPrecommit:
		return "CohortPrecommit"
	case events.EvCohortCommit:
		return "CohortCommit"
	case events.EvCohortAbort:
		return "CohortAbort"
	case events.EvLog:
		return "Log"
	default:
		return "Unknown"
	}
}
