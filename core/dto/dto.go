// Package dto defines the data transfer objects used for communication in
// distributed atomic commit protocols between coordinators and cohorts.
// These structures represent the messages exchanged during the two-phase
// and three-phase commit protocols.
package dto

type Protocol uint8

const (
	ProtocolTwoPhase Protocol = iota + 1
	ProtocolThreePhase
)

type Transaction struct {
	Key   string
	Value []byte
}

type Proposal struct {
	Height      uint64
	Protocol    Protocol
	Transaction Transaction
}

type ParticipantReply struct {
	Accepted bool
	Height   uint64
}

// ProposeRequest represents a proposal for a new transaction.
type ProposeRequest struct {
	Key    string // Key to be stored
	Value  []byte // Value to be stored
	Height uint64 // Transaction height/sequence number
}

// CommitRequest represents a commit or rollback request.
type CommitRequest struct {
	Height     uint64 // Transaction height/sequence number
	IsRollback bool   // Whether this is a rollback request
}

// ResponseType represents the type of response from a cohort.
type ResponseType int32

const (
	// ResponseTypeAck indicates successful acknowledgment.
	ResponseTypeAck ResponseType = iota
	// ResponseTypeNack indicates negative acknowledgment (rejection).
	ResponseTypeNack
)

// CohortResponse represents a response from a cohort node.
type CohortResponse struct {
	ResponseType
	Height uint64 // Current height of the cohort
}

// BroadcastRequest represents a request to be broadcast to all cohorts.
type BroadcastRequest struct {
	Key   string // Key to be stored
	Value []byte // Value to be stored
}

// BroadcastResponse represents a response to a broadcast request.
type BroadcastResponse struct {
	Type   ResponseType // Response type (ACK/NACK)
	Height uint64       // Height of the committed transaction
}

// AbortRequest represents a request to abort a transaction.
type AbortRequest struct {
	Height uint64 // Transaction height to abort
	Reason string // Reason for the abort
}

// Outcome represents the coordinator's recorded decision for a height.
type Outcome int32

const (
	// OutcomeUnknown means no decision has been recorded (yet).
	OutcomeUnknown Outcome = iota
	// OutcomeCommit means the transaction was committed.
	OutcomeCommit
	// OutcomeAbort means the transaction was aborted.
	OutcomeAbort
)

type FinalDecision struct {
	Height  uint64
	Outcome Outcome
	// RequirePrecommit preserves the ordinary 3PC PREPARED -> PRECOMMIT ->
	// COMMIT sequence when replaying a recovered or historical commit.
	RequirePrecommit bool
}
