package events

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

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

// Event carries protocol or log information from domain objects to the TUI.
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

// Emitter sends domain events to the TUI.
type Emitter interface {
	Emit(Event)
}

// NoopEmitter discards all events. Used in tests and --no-ui mode.
type NoopEmitter struct{}

func (NoopEmitter) Emit(Event) {}

// ChanEmitter is a non-blocking channel-based Emitter.
// Domain goroutines call Emit(); the bubbletea program reads via WaitForEvent().
// Events are dropped when the buffer is full so domain goroutines never block.
type ChanEmitter struct {
	ch chan Event
}

// NewChanEmitter creates a ChanEmitter with a 256-event buffer.
func NewChanEmitter() *ChanEmitter {
	return &ChanEmitter{ch: make(chan Event, 256)}
}

// Emit sends an event to the channel. Non-blocking; drops if buffer is full.
func (e *ChanEmitter) Emit(ev Event) {
	select {
	case e.ch <- ev:
	default:
	}
}

// WaitForEvent returns a bubbletea Cmd that blocks until the next event arrives.
// Re-issue this command in Update() after each received event to keep listening.
func (e *ChanEmitter) WaitForEvent() tea.Cmd {
	return func() tea.Msg {
		return <-e.ch
	}
}
