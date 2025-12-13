package walproto

import (
	"encoding/json"
	"fmt"
)

const (
	// system keys for protocol phases
	KeyPrepared  = "__tx:prepared"
	KeyPrecommit = "__tx:precommit"
	KeyCommit    = "__tx:commit"
	KeyAbort     = "__tx:abort"

	// stride is the multiplier for height to determine the starting WAL index
	Stride = 4
)

// WalTx represents the payload stored in the WAL.
type WalTx struct {
	Key   string `json:"k"`
	Value []byte `json:"v"`
}

// PreparedSlot returns the WAL index for the PREPARED phase at a given height.
func PreparedSlot(height uint64) uint64 {
	return height*Stride + 0
}

// PrecommitSlot returns the WAL index for the PRECOMMIT phase at a given height.
func PrecommitSlot(height uint64) uint64 {
	return height*Stride + 1
}

// CommitSlot returns the WAL index for the COMMIT phase at a given height.
func CommitSlot(height uint64) uint64 {
	return height*Stride + 2
}

// AbortSlot returns the WAL index for the ABORT phase at a given height.
func AbortSlot(height uint64) uint64 {
	return height*Stride + 3
}

// Encode serializes a WalTx into bytes.
func Encode(tx WalTx) ([]byte, error) {
	return json.Marshal(tx)
}

// Decode deserializes bytes into a WalTx.
func Decode(data []byte) (WalTx, error) {
	var tx WalTx
	err := json.Unmarshal(data, &tx)
	if err != nil {
		return WalTx{}, fmt.Errorf("failed to decode WalTx: %w", err)
	}
	return tx, nil
}
