// Package events provides a lightweight event bus used by core protocol code
// to feed the web visualization dashboard.
package events

import "time"

type EventKind uint8

const (
	EvCoordPropose    EventKind = iota // координатор начал фазу propose
	EvCoordPrecommit                   // координатор начал precommit (3PC)
	EvCoordCommit                      // координатор завершил commit
	EvCoordAbort                       // координатор инициировал abort
	EvCoordHeightSync                  // синхронизация height от когорты
	EvCohortPropose                    // когорта перешла в состояние propose
	EvCohortPrecommit                  // когорта перешла в precommit
	EvCohortCommit                     // когорта выполнила commit
	EvCohortAbort                      // когорта обработала abort
	EvLog                              // захваченная запись slog
)

// Event carries protocol or log information from domain objects to the dashboard.
type Event struct {
	Kind      EventKind
	Timestamp time.Time
	Key       string // transaction key
	Height    uint64
	Cohort    string // cohort address (coordinator-side per-cohort events)
	Result    string // "ok" / "nack" / "abort"
	Message   string // text for EvLog events
	Level     string // "INFO" / "WARN" / "ERROR" / "DEBUG"
}

// Emitter sends domain events to the dashboard.
type Emitter interface {
	Emit(Event)
}

// NoopEmitter discards all events. Used in tests and when the dashboard is disabled.
type NoopEmitter struct{}

func (NoopEmitter) Emit(Event) {}
