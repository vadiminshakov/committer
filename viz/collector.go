package viz

import (
	"sync"
	"time"

	"github.com/vadiminshakov/committer/events"
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

// eventKindNames maps protocol event kinds to dashboard labels.
var eventKindNames = map[events.EventKind]string{
	events.EvCoordPropose:    "CoordPropose",
	events.EvCoordPrecommit:  "CoordPrecommit",
	events.EvCoordCommit:     "CoordCommit",
	events.EvCoordAbort:      "CoordAbort",
	events.EvCohortPropose:   "CohortPropose",
	events.EvCohortPrecommit: "CohortPrecommit",
	events.EvCohortCommit:    "CohortCommit",
	events.EvCohortAbort:     "CohortAbort",
	events.EvLog:             "Log",
}

func NewCollector(inner events.Emitter) *Collector {
	if inner == nil {
		inner = events.NoopEmitter{}
	}

	return &Collector{inner: inner}
}

func (c *Collector) Emit(event events.Event) {
	c.inner.Emit(event)

	dto := EventDTO{
		Kind:      uint8(event.Kind),
		KindName:  kindName(event.Kind),
		Timestamp: event.Timestamp,
		Key:       event.Key,
		Height:    event.Height,
		Cohort:    event.Cohort,
		Result:    event.Result,
		Message:   event.Message,
		Level:     event.Level,
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
	if name, ok := eventKindNames[k]; ok {
		return name
	}

	return "Unknown"
}
