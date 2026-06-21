package viz

import (
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/events"
)

func TestServerAPICohorts(t *testing.T) {
	collector := NewCollector(nil)
	conf := &config.Config{
		Role:        "coordinator",
		Nodeaddr:    "localhost:3000",
		Coordinator: "",
		Cohorts:     []string{"localhost:3001", "localhost:3002"},
		CommitType:  "two-phase",
	}
	srv := NewServer(collector, conf, 0)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/cohorts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(srv.config.Cohorts)
	})

	req := httptest.NewRequest("GET", "/api/cohorts", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result []string
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(result) != 2 || result[0] != "localhost:3001" || result[1] != "localhost:3002" {
		t.Fatalf("unexpected cohorts: %v", result)
	}
}

func TestServerAPICohortsEmpty(t *testing.T) {
	collector := NewCollector(nil)
	conf := &config.Config{
		Role:    "cohort",
		Cohorts: []string{},
	}
	srv := NewServer(collector, conf, 0)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/cohorts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(srv.config.Cohorts)
	})

	req := httptest.NewRequest("GET", "/api/cohorts", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result []string
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil slice, got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %v", result)
	}
}

func TestServerCohortsWithNodeAddr(t *testing.T) {
	collector := NewCollector(nil)
	conf := &config.Config{
		Role:     "cohort",
		Nodeaddr: "localhost:3001",
		Cohorts:  []string{},
	}
	srv := NewServer(collector, conf, 0)

	result := srv.cohorts()
	if len(result) != 1 || result[0] != "localhost:3001" {
		t.Fatalf("expected [localhost:3001], got %v", result)
	}
}

func TestServerCohortsFromEvents(t *testing.T) {
	collector := NewCollector(nil)
	collector.Emit(events.Event{Cohort: "localhost:3002", Kind: events.EvCoordPropose, Height: 1, Result: "ok"})
	collector.Emit(events.Event{Cohort: "localhost:3003", Kind: events.EvCoordPrecommit, Height: 1, Result: "nack"})
	conf := &config.Config{
		Role:     "coordinator",
		Nodeaddr: "localhost:3000",
		Cohorts:  []string{"localhost:3001"},
	}
	srv := NewServer(collector, conf, 0)

	result := srv.cohorts()
	if len(result) != 3 {
		t.Fatalf("expected 3 cohorts (1 configured + 2 from events), got %d: %v", len(result), result)
	}
}

func TestServerMuxRouting(t *testing.T) {
	collector := NewCollector(nil)
	conf := &config.Config{
		Role:        "coordinator",
		Nodeaddr:    "localhost:3000",
		Cohorts:     []string{"localhost:3001", "localhost:3002"},
		Coordinator: "",
		CommitType:  "two-phase",
	}
	srv := NewServer(collector, conf, 0)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(srv.collector.Events())
	})
	mux.HandleFunc("/api/cohorts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(srv.config.Cohorts)
	})
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"role":        srv.config.Role,
			"nodeaddr":    srv.config.Nodeaddr,
			"cohorts":     srv.config.Cohorts,
			"coordinator": srv.config.Coordinator,
			"commitType":  srv.config.CommitType,
		})
	})
	staticSub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Test /api/cohorts
	resp, err := http.Get(ts.URL + "/api/cohorts")
	if err != nil {
		t.Fatalf("GET /api/cohorts failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/cohorts: expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var cohortsResult []string
	if err := json.Unmarshal(body, &cohortsResult); err != nil {
		t.Fatalf("GET /api/cohorts: failed to parse body %q: %v", string(body), err)
	}
	if len(cohortsResult) != 2 || cohortsResult[0] != "localhost:3001" {
		t.Fatalf("GET /api/cohorts: unexpected: %v", cohortsResult)
	}

	// Test /api/config
	resp2, err := http.Get(ts.URL + "/api/config")
	if err != nil {
		t.Fatalf("GET /api/config failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/config: expected 200, got %d", resp2.StatusCode)
	}

	body2, _ := io.ReadAll(resp2.Body)
	var cfgResult map[string]interface{}
	if err := json.Unmarshal(body2, &cfgResult); err != nil {
		t.Fatalf("GET /api/config: failed to parse: %v", err)
	}
	if cfgResult["role"] != "coordinator" {
		t.Fatalf("GET /api/config: unexpected role: %v", cfgResult["role"])
	}

	// Test /api/events
	resp3, err := http.Get(ts.URL + "/api/events")
	if err != nil {
		t.Fatalf("GET /api/events failed: %v", err)
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/events: expected 200, got %d", resp3.StatusCode)
	}

	// Test / serves index.html
	resp4, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	defer resp4.Body.Close()

	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("GET /: expected 200, got %d", resp4.StatusCode)
	}
}
