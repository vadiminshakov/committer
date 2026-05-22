package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/tui/events"
	"github.com/vadiminshakov/committer/tui/model"
)

// NewProgram creates a bubbletea program for the appropriate role (coordinator or cohort).
func NewProgram(conf *config.Config, src *events.ChanEmitter) *tea.Program {
	var m tea.Model
	if conf.Role == "coordinator" {
		m = model.NewCoordinatorModel(conf, src)
	} else {
		m = model.NewCohortModel(conf, src)
	}
	return tea.NewProgram(m, tea.WithAltScreen())
}
