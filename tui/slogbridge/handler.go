package slogbridge

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/vadiminshakov/committer/tui/events"
)

type emitter interface {
	Emit(e events.Event)
}

// Handler is a slog.Handler that forwards log records to an events.Emitter
// as EvLog events, so the TUI can display them in its event log panel.
type Handler struct {
	emitter emitter
	level   slog.Level
	attrs   []slog.Attr
	groups  []string
}

func NewHandler(emitter emitter, level slog.Level) *Handler {
	return &Handler{emitter: emitter, level: level}
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder
	sb.WriteString(r.Message)

	for _, a := range h.attrs {
		sb.WriteByte(' ')
		sb.WriteString(a.Key)
		sb.WriteByte('=')
		sb.WriteString(fmt.Sprintf("%v", a.Value))
	}

	r.Attrs(func(a slog.Attr) bool {
		sb.WriteByte(' ')
		sb.WriteString(a.Key)
		sb.WriteByte('=')
		sb.WriteString(fmt.Sprintf("%v", a.Value))
		return true
	})

	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}

	h.emitter.Emit(events.Event{
		Kind:      events.EvLog,
		Timestamp: ts,
		Level:     r.Level.String(),
		Message:   sb.String(),
	})
	return nil
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &Handler{emitter: h.emitter, level: h.level, attrs: newAttrs, groups: h.groups}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	newGroups := make([]string, len(h.groups)+1)
	copy(newGroups, h.groups)
	newGroups[len(h.groups)] = name
	return &Handler{emitter: h.emitter, level: h.level, attrs: h.attrs, groups: newGroups}
}
