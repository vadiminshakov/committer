package walrecord

import (
	"encoding/binary"
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
	Key   string
	Value []byte
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
// Format: [KeyLen(4 bytes)] [KeyBytes] [ValueLen(4 bytes)] [ValueBytes]
func Encode(tx WalTx) ([]byte, error) {
	keyLen := uint32(len(tx.Key))
	valLen := uint32(len(tx.Value))

	// 4 bytes for key len + key bytes + 4 bytes for val len + val bytes
	buf := make([]byte, 4+keyLen+4+valLen)

	binary.BigEndian.PutUint32(buf[0:4], keyLen)
	copy(buf[4:4+keyLen], tx.Key)

	binary.BigEndian.PutUint32(buf[4+keyLen:4+keyLen+4], valLen)
	copy(buf[4+keyLen+4:], tx.Value)

	return buf, nil
}

// Decode deserializes bytes into a WalTx.
func Decode(data []byte) (WalTx, error) {
	if len(data) < 4 {
		return WalTx{}, fmt.Errorf("data too short for key length")
	}

	keyLen := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < 4+keyLen+4 {
		return WalTx{}, fmt.Errorf("data too short for key and value length")
	}

	key := string(data[4 : 4+keyLen])

	valLen := binary.BigEndian.Uint32(data[4+keyLen : 4+keyLen+4])
	if uint32(len(data)) < 4+keyLen+4+valLen {
		return WalTx{}, fmt.Errorf("data too short for value body")
	}

	value := make([]byte, valLen)
	copy(value, data[4+keyLen+4:4+keyLen+4+valLen])

	return WalTx{
		Key:   key,
		Value: value,
	}, nil
}
