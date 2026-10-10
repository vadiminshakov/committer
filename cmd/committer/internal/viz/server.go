package viz

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
)

// Node roles, as in Node.Role.
const (
	roleCoordinator = "coordinator"
	roleCohort      = "cohort"
)

// Node describes the node whose protocol messages are shown.
type Node struct {
	Role        string // roleCoordinator or roleCohort
	Addr        string
	Coordinator string
	Cohorts     []string
	CommitType  string
}

type Server struct {
	collector *Collector
	node      Node
	port      int
}

//go:embed static
var staticFS embed.FS

func NewServer(collector *Collector, node Node, port int) *Server {
	return &Server{collector: collector, node: node, port: port}
}

func (s *Server) Start() {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")

		if err := json.NewEncoder(w).Encode(s.collector.Events()); err != nil {
			slog.Warn("failed to encode events response", "err", err)
		}
	})

	mux.HandleFunc("/api/cohorts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")

		if err := json.NewEncoder(w).Encode(s.cohorts()); err != nil {
			slog.Warn("failed to encode cohorts response", "err", err)
		}
	})

	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")

		if err := json.NewEncoder(w).Encode(map[string]any{
			"role":    s.node.Role,
			"addr":    s.node.Addr,
			"cohorts": s.node.Cohorts,
			// The key intentionally matches the coordinator role name.
			roleCoordinator: s.node.Coordinator,
			"commitType":    s.node.CommitType,
		}); err != nil {
			slog.Warn("failed to encode config response", "err", err)
		}
	})

	staticSub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	addr := fmt.Sprintf(":%d", s.port)

	slog.Info("Visualization server started", "addr", addr, "role", s.node.Role, "cohorts", s.node.Cohorts)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("Visualization server error", "err", err)
		}
	}()
}

func (s *Server) cohorts() []string {
	seen := make(map[string]bool)
	for _, c := range s.node.Cohorts {
		seen[c] = true
	}

	for _, e := range s.collector.Events() {
		if e.Cohort != "" {
			seen[e.Cohort] = true
		}
	}

	if s.node.Role == roleCohort && s.node.Addr != "" {
		seen[s.node.Addr] = true
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	return out
}
