package viz

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/vadiminshakov/committer/config"
)

//go:embed static
var staticFS embed.FS

type Server struct {
	collector *Collector
	config    *config.Config
	port      int
}

func NewServer(collector *Collector, conf *config.Config, port int) *Server {
	return &Server{collector: collector, config: conf, port: port}
}

func (s *Server) Start() {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		json.NewEncoder(w).Encode(s.collector.Events())
	})

	mux.HandleFunc("/api/cohorts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		json.NewEncoder(w).Encode(s.cohorts())
	})

	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"role":        s.config.Role,
			"nodeaddr":    s.config.Nodeaddr,
			"cohorts":     s.config.Cohorts,
			"coordinator": s.config.Coordinator,
			"commitType":  s.config.CommitType,
		})
	})

	staticSub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	addr := fmt.Sprintf(":%d", s.port)
	slog.Info("Visualization server started", "addr", addr, "role", s.config.Role, "cohorts", s.config.Cohorts)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("Visualization server error", "err", err)
		}
	}()
}

func (s *Server) cohorts() []string {
	seen := make(map[string]bool)
	for _, c := range s.config.Cohorts {
		seen[c] = true
	}
	for _, e := range s.collector.Events() {
		if e.Cohort != "" {
			seen[e.Cohort] = true
		}
	}
	if s.config.Role == "cohort" && s.config.Nodeaddr != "" {
		seen[s.config.Nodeaddr] = true
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	return out
}
