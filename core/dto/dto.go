// Package dto provides data transfer objects for inter-node communication.
//
// This package defines the request and response structures used in the
// distributed consensus protocols between coordinators and cohorts.
package dto

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
