package hooks

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/dto"
)

// MetricsHook collects metrics about propose and commit operations
type MetricsHook struct {
	proposeCount uint64
	commitCount  uint64
	startTime    time.Time
}

// NewMetricsHook creates a new metrics hook
func NewMetricsHook() *MetricsHook {
	return &MetricsHook{
		startTime: time.Now(),
	}
}

// OnPropose increments propose counter and logs metrics
func (m *MetricsHook) OnPropose(req *dto.ProposeRequest) bool {
	m.proposeCount++
	log.WithFields(log.Fields{
		"height":        req.Height,
		"propose_count": m.proposeCount,
		"uptime":        time.Since(m.startTime),
	}).Info("Metrics: propose operation")
	return true
}

// OnCommit increments commit counter and logs metrics
func (m *MetricsHook) OnCommit(req *dto.CommitRequest) bool {
	m.commitCount++
	log.WithFields(log.Fields{
		"height":       req.Height,
		"commit_count": m.commitCount,
		"uptime":       time.Since(m.startTime),
	}).Info("Metrics: commit operation")
	return true
}

// GetStats returns current statistics
func (m *MetricsHook) GetStats() (uint64, uint64, time.Duration) {
	return m.proposeCount, m.commitCount, time.Since(m.startTime)
}

// ValidationHook validates requests before processing
type ValidationHook struct {
	maxKeyLength   int
	maxValueLength int
}

// NewValidationHook creates a new validation hook
func NewValidationHook(maxKeyLength, maxValueLength int) *ValidationHook {
	return &ValidationHook{
		maxKeyLength:   maxKeyLength,
		maxValueLength: maxValueLength,
	}
}

// OnPropose validates the propose request
func (v *ValidationHook) OnPropose(req *dto.ProposeRequest) bool {
	if len(req.Key) > v.maxKeyLength {
		log.Errorf("Key too long: %d > %d", len(req.Key), v.maxKeyLength)
		return false
	}

	if len(req.Value) > v.maxValueLength {
		log.Errorf("Value too long: %d > %d", len(req.Value), v.maxValueLength)
		return false
	}

	log.Debugf("Validation passed for key: %s", req.Key)
	return true
}

// OnCommit validates the commit request
func (v *ValidationHook) OnCommit(req *dto.CommitRequest) bool {
	if req.Height == 0 {
		log.Error("Invalid height: cannot be zero")
		return false
	}

	log.Debugf("Commit validation passed for height: %d", req.Height)
	return true
}

// AuditHook logs all operations for audit purposes
type AuditHook struct {
	logFile string
}

// NewAuditHook creates a new audit hook
func NewAuditHook(logFile string) *AuditHook {
	return &AuditHook{
		logFile: logFile,
	}
}

// OnPropose logs propose operations
func (a *AuditHook) OnPropose(req *dto.ProposeRequest) bool {
	auditMsg := fmt.Sprintf("[AUDIT] PROPOSE - Height: %d, Key: %s, Value: %s, Time: %s",
		req.Height, req.Key, string(req.Value), time.Now().Format(time.RFC3339))

	log.WithField("audit", true).Info(auditMsg)
	// Here you could also write to a file if needed
	return true
}

// OnCommit logs commit operations
func (a *AuditHook) OnCommit(req *dto.CommitRequest) bool {
	auditMsg := fmt.Sprintf("[AUDIT] COMMIT - Height: %d, Time: %s",
		req.Height, time.Now().Format(time.RFC3339))

	log.WithField("audit", true).Info(auditMsg)
	// Here you could also write to a file if needed
	return true
}
