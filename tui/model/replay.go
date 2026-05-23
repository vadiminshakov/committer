package model

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/vadiminshakov/committer/tui/events"
)

type replayTickMsg struct{}

type replayState struct {
	active bool
	evs    []events.Event // snapshot of eventHistory taken at replay start
	cursor int
	delay  time.Duration
	paused bool
}

func newReplayState() replayState {
	return replayState{delay: 600 * time.Millisecond}
}

// start snapshots the event history and begins ticking.
func (r *replayState) start(history []events.Event) tea.Cmd {
	r.active = true
	r.paused = false
	r.cursor = 0
	r.evs = make([]events.Event, len(history))
	copy(r.evs, history)
	return r.nextTick()
}

func (r *replayState) stop() { r.active = false }

func (r *replayState) nextTick() tea.Cmd {
	if !r.active || r.paused || r.cursor >= len(r.evs) {
		return nil
	}
	d := r.delay
	return tea.Tick(d, func(time.Time) tea.Msg { return replayTickMsg{} })
}

func (r *replayState) done() bool { return r.cursor >= len(r.evs) }
