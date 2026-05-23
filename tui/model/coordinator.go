package model

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/tui/events"
)

const maxLogLines = 500

// cohortStatus tracks per-cohort result for each phase.
type cohortStatus struct {
	propose    string // "", "ok", "nack"
	precommit  string
	commit     string
}

// CoordinatorModel is the bubbletea model for the coordinator TUI screen.
type CoordinatorModel struct {
	conf       *config.Config
	emitter    *events.ChanEmitter
	width      int
	height     int
	// current transaction
	txKey      string
	txHeight   uint64
	// phase pipeline state: "", "propose", "precommit", "commit", "abort"
	phase      string
	aborted    bool
	// per-cohort status
	cohorts    map[string]*cohortStatus
	cohortKeys []string // ordered keys for stable rendering
	// event log
	logLines   []string
	viewport   viewport.Model
	vpReady    bool
}

func NewCoordinatorModel(conf *config.Config, src *events.ChanEmitter) CoordinatorModel {
	cohortMap := make(map[string]*cohortStatus, len(conf.Cohorts))
	keys := make([]string, 0, len(conf.Cohorts))
	for _, addr := range conf.Cohorts {
		cohortMap[addr] = &cohortStatus{}
		keys = append(keys, addr)
	}
	return CoordinatorModel{
		conf:       conf,
		emitter:    src,
		cohorts:    cohortMap,
		cohortKeys: keys,
	}
}

func (m CoordinatorModel) Init() tea.Cmd {
	return m.emitter.WaitForEvent()
}

func (m CoordinatorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch ev := msg.(type) {
	case events.Event:
		m = m.handleEvent(ev)
		if m.vpReady {
			m.viewport.SetContent(strings.Join(m.logLines, "\n"))
			m.viewport.GotoBottom()
		}
		return m, m.emitter.WaitForEvent()

	case tea.WindowSizeMsg:
		m.width = ev.Width
		m.height = ev.Height
		logHeight := m.height - 22 // header + txSection + cohortTable + logTitle
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

	case tea.KeyMsg:
		switch ev.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "j":
			m.viewport.ScrollDown(1)
		case "k":
			m.viewport.ScrollUp(1)
		}
	}
	return m, nil
}

func (m CoordinatorModel) handleEvent(ev events.Event) CoordinatorModel {
	switch ev.Kind {
	case events.EvCoordPropose:
		if ev.Cohort == "" {
			// phase-level event
			m.txKey = ev.Key
			m.txHeight = ev.Height
			m.phase = "propose"
			m.aborted = false
			// reset cohort statuses for new tx
			for _, s := range m.cohorts {
				*s = cohortStatus{}
			}
		} else if s, ok := m.cohorts[ev.Cohort]; ok {
			s.propose = ev.Result
		}

	case events.EvCoordPrecommit:
		if ev.Cohort == "" {
			m.phase = "precommit"
		} else if s, ok := m.cohorts[ev.Cohort]; ok {
			s.precommit = ev.Result
		}

	case events.EvCoordCommit:
		if ev.Cohort == "" {
			if ev.Result == "ok" {
				m.phase = "committed"
				m.txHeight = ev.Height
			} else {
				m.phase = "commit"
			}
		} else if s, ok := m.cohorts[ev.Cohort]; ok {
			s.commit = ev.Result
		}

	case events.EvCoordAbort:
		m.phase = "abort"
		m.aborted = true

	case events.EvCoordHeightSync:
		m.txHeight = ev.Height

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

// Styles

var (
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("57")).
			Padding(0, 1)

	sectionStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1)

	phaseActive = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("10")). // bright green
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("10")).
			Padding(0, 1)

	phaseDone = lipgloss.NewStyle().
			Foreground(lipgloss.Color("10")).
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("10")).
			Padding(0, 1)

	phasePending = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1)

	phaseAbort = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("9")). // bright red
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("9")).
			Padding(0, 1)

	dimStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))

	logHeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("12"))
)

func (m CoordinatorModel) View() string {
	if m.width == 0 {
		return "initializing…"
	}

	proto := strings.ToUpper(m.conf.CommitType)

	// Header
	header := headerStyle.Render(fmt.Sprintf(
		" COMMITTER  COORDINATOR  addr:%s  cohorts:%s  proto:%s  h:%d ",
		m.conf.Nodeaddr, strings.Join(m.conf.Cohorts, ","), proto, m.txHeight,
	))

	// Active transaction
	var txInfo string
	if m.txKey != "" {
		txInfo = fmt.Sprintf("  key: %q   height: %d", m.txKey, m.txHeight)
	} else {
		txInfo = dimStyle.Render("  waiting for transactions…")
	}

	// Phase pipeline
	phases := m.renderPhases(proto)

	txSection := sectionStyle.Width(m.width - 4).Render(
		lipgloss.JoinVertical(lipgloss.Left, txInfo, "", phases),
	)

	// Cohort status table
	cohortTable := m.renderCohortTable(proto)

	// Event log
	logTitle := logHeaderStyle.Render("Event Log") + dimStyle.Render("  (j/k scroll, q quit)")
	logSection := sectionStyle.Width(m.width - 4).Render(
		lipgloss.JoinVertical(lipgloss.Left, logTitle, m.viewport.View()),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		txSection,
		cohortTable,
		logSection,
	)
}

func (m CoordinatorModel) renderPhases(proto string) string {
	arrow := dimStyle.Render(" ──► ")

	styleFor := func(name string) lipgloss.Style {
		switch {
		case m.aborted && name == "abort":
			return phaseAbort
		case m.aborted:
			return phasePending
		case name == "abort":
			return phasePending
		case m.phase == "committed" && (name == "propose" || name == "precommit" || name == "commit"):
			return phaseDone
		case m.phase == name:
			return phaseActive
		case phaseOrder(m.phase) > phaseOrder(name):
			return phaseDone
		default:
			return phasePending
		}
	}

	propose := styleFor("propose").Render("PROPOSE")
	commit := styleFor("commit").Render("COMMIT")
	abort := styleFor("abort").Render("ABORT")

	if proto == "THREE-PHASE" {
		precommit := styleFor("precommit").Render("PRECOMMIT")
		return lipgloss.JoinHorizontal(lipgloss.Center,
			propose, arrow, precommit, arrow, commit, dimStyle.Render("  /  "), abort,
		)
	}
	return lipgloss.JoinHorizontal(lipgloss.Center,
		propose, arrow, commit, dimStyle.Render("  /  "), abort,
	)
}

func phaseOrder(phase string) int {
	switch phase {
	case "propose":
		return 1
	case "precommit":
		return 2
	case "commit", "committed":
		return 3
	case "abort":
		return 4
	}
	return 0
}

// cellStyle applies a fixed visible width to a cell — safe with ANSI-styled content.
var cellBase = lipgloss.NewStyle().PaddingRight(2)

func tableCell(text string, style lipgloss.Style, width int) string {
	return style.Width(width).Render(text)
}

func (m CoordinatorModel) renderCohortTable(proto string) string {
	if len(m.cohortKeys) == 0 {
		return ""
	}

	threePhase := proto == "THREE-PHASE"
	w0, w1, w2, w3 := 24, 10, 12, 10

	headerStyle2 := cellBase.Bold(true).Foreground(lipgloss.Color("240"))
	addrStyle := cellBase.Foreground(lipgloss.Color("255"))

	buildHeader := func() string {
		cells := []string{
			tableCell("COHORT", headerStyle2, w0),
			tableCell("PROPOSE", headerStyle2, w1),
		}
		if threePhase {
			cells = append(cells, tableCell("PRECOMMIT", headerStyle2, w2))
		}
		cells = append(cells, tableCell("COMMIT", headerStyle2, w3))
		return "  " + lipgloss.JoinHorizontal(lipgloss.Top, cells...)
	}

	buildDataRow := func(addr, propose, precommit, commit string) string {
		cells := []string{
			tableCell(addr, addrStyle, w0),
			tableCell(propose, resultStyle(propose), w1),
		}
		if threePhase {
			cells = append(cells, tableCell(precommit, resultStyle(precommit), w2))
		}
		cells = append(cells, tableCell(commit, resultStyle(commit), w3))
		return "  " + lipgloss.JoinHorizontal(lipgloss.Top, cells...)
	}

	rows := []string{buildHeader()}
	for _, addr := range m.cohortKeys {
		s := m.cohorts[addr]
		rows = append(rows, buildDataRow(
			addr,
			resultText(s.propose),
			resultText(s.precommit),
			resultText(s.commit),
		))
	}

	return sectionStyle.Width(m.width - 4).Render(strings.Join(rows, "\n"))
}

// resultText returns plain text for a result value (for use with tableCell).
func resultText(r string) string {
	switch r {
	case "ok":
		return "ok"
	case "nack":
		return "nack"
	default:
		return "-"
	}
}

// resultStyle returns the lipgloss style for a result cell value.
func resultStyle(r string) lipgloss.Style {
	switch r {
	case "ok":
		return cellBase.Foreground(lipgloss.Color("10"))
	case "nack":
		return cellBase.Foreground(lipgloss.Color("9"))
	default:
		return cellBase.Foreground(lipgloss.Color("240"))
	}
}
