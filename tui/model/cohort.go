package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/tui/events"
)

// CohortModel is the bubbletea model for the cohort TUI screen.
type CohortModel struct {
	conf     *config.Config
	emitter  *events.ChanEmitter
	width    int
	height   int
	// FSM current state: "propose", "precommit", "commit"
	fsmState string
	// last completed transaction
	lastKey    string
	lastHeight uint64
	lastResult string // "COMMITTED" / "ABORTED"
	// event log
	logLines     []string
	eventHistory []events.Event
	viewport     viewport.Model
	vpReady      bool
	replay       replayState
}

func NewCohortModel(conf *config.Config, src *events.ChanEmitter) CohortModel {
	return CohortModel{
		conf:     conf,
		emitter:  src,
		fsmState: "propose",
		replay:   newReplayState(),
	}
}

func (m CohortModel) Init() tea.Cmd {
	return m.emitter.WaitForEvent()
}

func (m CohortModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch ev := msg.(type) {
	case events.Event:
		m.eventHistory = append(m.eventHistory, ev)
		if m.replay.active {
			return m, nil
		}
		m = m.handleEvent(ev)
		if m.vpReady {
			m.viewport.SetContent(strings.Join(m.logLines, "\n"))
			m.viewport.GotoBottom()
		}
		return m, m.emitter.WaitForEvent()

	case tea.WindowSizeMsg:
		m.width = ev.Width
		m.height = ev.Height
		logHeight := m.height - 22
		if logHeight < 3 {
			logHeight = 3
		}
		if !m.vpReady {
			m.viewport = viewport.New(m.width-4, logHeight)
			m.vpReady = true
		} else {
			m.viewport.Width = m.width - 4
			m.viewport.Height = logHeight
		}
		m.viewport.SetContent(strings.Join(m.logLines, "\n"))
		m.viewport.GotoBottom()

	case replayTickMsg:
		if !m.replay.active || m.replay.done() {
			return m, nil
		}
		m = m.handleEvent(m.replay.evs[m.replay.cursor])
		m.replay.cursor++
		if m.vpReady {
			content := strings.Join(m.logLines, "\n")
			if m.replay.done() {
				content += "\n" + dimStyle.Render("  ── replay done ── press r to exit ──")
			}
			m.viewport.SetContent(content)
			m.viewport.GotoBottom()
		}
		return m, m.replay.nextTick()

	case tea.KeyMsg:
		switch ev.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "r":
			if m.replay.active {
				m.replay.stop()
				m = m.replayApplyHistory()
				if m.vpReady {
					m.viewport.SetContent(strings.Join(m.logLines, "\n"))
					m.viewport.GotoBottom()
				}
				return m, m.emitter.WaitForEvent()
			}
			if len(m.eventHistory) == 0 {
				return m, nil
			}
			cmd := m.replay.start(m.eventHistory)
			m = m.replayResetState()
			if m.vpReady {
				m.viewport.SetContent("")
			}
			return m, cmd
		case "esc":
			if m.replay.active {
				m.replay.stop()
				m = m.replayApplyHistory()
				if m.vpReady {
					m.viewport.SetContent(strings.Join(m.logLines, "\n"))
					m.viewport.GotoBottom()
				}
				return m, m.emitter.WaitForEvent()
			}
		case "+", "=":
			if m.replay.active && m.replay.delay > 50*time.Millisecond {
				m.replay.delay /= 2
			}
		case "-":
			if m.replay.active {
				m.replay.delay *= 2
			}
		case " ":
			if m.replay.active {
				m.replay.paused = !m.replay.paused
				return m, m.replay.nextTick()
			}
		case "j":
			m.viewport.ScrollDown(1)
		case "k":
			m.viewport.ScrollUp(1)
		}
	}
	return m, nil
}

func (m CohortModel) handleEvent(ev events.Event) CohortModel {
	switch ev.Kind {
	case events.EvCohortPropose:
		m.fsmState = "propose"
		if ev.Key != "" {
			m.lastKey = ev.Key
		}
		m.lastHeight = ev.Height

	case events.EvCohortPrecommit:
		m.fsmState = "precommit"

	case events.EvCohortCommit:
		if ev.Result == "ok" {
			m.fsmState = "committed"
			m.lastResult = "COMMITTED"
			m.lastHeight = ev.Height
		} else {
			m.fsmState = "commit"
		}

	case events.EvCohortAbort:
		m.fsmState = "propose"
		m.lastResult = "ABORTED"
		m.lastHeight = ev.Height

	case events.EvLog:
		line := fmt.Sprintf("%s %-5s %s",
			ev.Timestamp.Format("15:04:05"),
			ev.Level,
			ev.Message,
		)
		m.logLines = append(m.logLines, line)
		if len(m.logLines) > maxLogLines {
			m.logLines = m.logLines[len(m.logLines)-maxLogLines:]
		}
	}
	return m
}

func (m CohortModel) replayResetState() CohortModel {
	m.fsmState = "propose"
	m.lastKey = ""
	m.lastHeight = 0
	m.lastResult = ""
	m.logLines = nil
	return m
}

func (m CohortModel) replayApplyHistory() CohortModel {
	m = m.replayResetState()
	for _, ev := range m.eventHistory {
		m = m.handleEvent(ev)
	}
	return m
}

// Styles

var (
	cohortHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("24")).
				Padding(0, 1)

	fsmActive = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("11")).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("11")).
			Padding(0, 1)

	fsmDone = lipgloss.NewStyle().
			Foreground(lipgloss.Color("11")).
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("11")).
			Padding(0, 1)

	fsmInactive = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1)

	resultCommit = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("10"))
	resultAbort  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("9"))
)

func (m CohortModel) View() string {
	if m.width == 0 {
		return "initializing…"
	}

	proto := strings.ToUpper(m.conf.CommitType)

	// Header
	header := cohortHeaderStyle.Render(fmt.Sprintf(
		" COMMITTER  COHORT  addr:%s  coord:%s  proto:%s  h:%d ",
		m.conf.Nodeaddr, m.conf.Coordinator, proto, m.lastHeight,
	))

	// FSM diagram
	fsmDiagram := m.renderFSM(proto)
	fsmSection := sectionStyle.Width(m.width - 4).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().PaddingLeft(2).Render(fsmDiagram),
		),
	)

	// Last transaction
	var lastTxContent string
	if m.lastResult != "" {
		resultStyled := resultCommit.Render(m.lastResult)
		if m.lastResult == "ABORTED" {
			resultStyled = resultAbort.Render(m.lastResult)
		}
		lastTxContent = fmt.Sprintf("  key:    %s\n  height: %d\n  result: %s",
			m.lastKey, m.lastHeight, resultStyled)
	} else {
		lastTxContent = dimStyle.Render("  no completed transactions yet")
	}
	lastTxSection := sectionStyle.Width(m.width - 4).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			dimStyle.Render("  Last Transaction"),
			lastTxContent,
		),
	)

	// Event log
	hint := "  (j/k scroll, q quit, r replay)"
	if m.replay.active {
		status := fmt.Sprintf("REPLAY %d/%d", m.replay.cursor, len(m.replay.evs))
		if m.replay.paused {
			status += " PAUSED"
		}
		hint = fmt.Sprintf("  (%s  +/- speed  space pause  r/esc exit)", status)
	}
	logTitle := logHeaderStyle.Render("Event Log") + dimStyle.Render(hint)
	logSection := sectionStyle.Width(m.width - 4).Render(
		lipgloss.JoinVertical(lipgloss.Left, logTitle, m.viewport.View()),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		fsmSection,
		lastTxSection,
		logSection,
	)
}

func (m CohortModel) renderFSM(proto string) string {
	arrow := dimStyle.Render(" ──► ")

	threePhase := proto == "THREE-PHASE"

	proposeStyle := fsmInactive
	precommitStyle := fsmInactive
	commitStyle := fsmInactive

	switch m.fsmState {
	case "committed":
		proposeStyle = fsmDone
		precommitStyle = fsmDone
		commitStyle = fsmDone
	case "propose":
		proposeStyle = fsmActive
	case "precommit":
		proposeStyle = fsmDone
		precommitStyle = fsmActive
	case "commit":
		proposeStyle = fsmDone
		precommitStyle = fsmDone
		commitStyle = fsmActive
	}

	propose := proposeStyle.Render("PROPOSE")
	commit := commitStyle.Render("COMMIT")

	if threePhase {
		precommit := precommitStyle.Render("PRECOMMIT")
		return lipgloss.JoinHorizontal(lipgloss.Center,
			propose, arrow, precommit, arrow, commit,
		)
	}
	return lipgloss.JoinHorizontal(lipgloss.Center,
		propose, arrow, commit,
	)
}
