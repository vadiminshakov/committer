package wal

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const txPrefix = "__tx:"

const (
	PhaseKeyPrepared  = "prepared"
	PhaseKeyPrecommit = "precommit"
	PhaseKeyCommit    = "commit"
	PhaseKeyAbort     = "abort"
)

func PreparedKey(height uint64) string  { return txPrefix + "prepared:" + strconv.FormatUint(height, 10) }
func PrecommitKey(height uint64) string { return txPrefix + "precommit:" + strconv.FormatUint(height, 10) }
func CommitKey(height uint64) string    { return txPrefix + "commit:" + strconv.FormatUint(height, 10) }
func AbortKey(height uint64) string     { return txPrefix + "abort:" + strconv.FormatUint(height, 10) }

// ParseKey extracts the phase and height from a key like "__tx:prepared:10".
// Returns ok=false for keys that are not protocol-phase records.
func ParseKey(key string) (phase string, height uint64, ok bool) {
	if !strings.HasPrefix(key, txPrefix) {
		return "", 0, false
	}
	rest := key[len(txPrefix):]
	idx := strings.LastIndex(rest, ":")
	if idx < 0 {
		return "", 0, false
	}
	phase = rest[:idx]
	h, err := strconv.ParseUint(rest[idx+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return phase, h, true
}

// Tx represents the payload stored in the WAL.
type Tx struct {
	Key   string
	Value []byte
}

// Encode serializes a Tx into bytes.
// Format: [KeyLen(4 bytes)] [KeyBytes] [ValueLen(4 bytes)] [ValueBytes]
func Encode(tx Tx) ([]byte, error) {
	keyLen := uint32(len(tx.Key))
	valLen := uint32(len(tx.Value))

	buf := make([]byte, 4+keyLen+4+valLen)

	binary.BigEndian.PutUint32(buf[0:4], keyLen)
	copy(buf[4:4+keyLen], tx.Key)

	binary.BigEndian.PutUint32(buf[4+keyLen:4+keyLen+4], valLen)
	copy(buf[4+keyLen+4:], tx.Value)

	return buf, nil
}

// Decode deserializes bytes into a Tx.
func Decode(data []byte) (Tx, error) {
	if len(data) < 4 {
		return Tx{}, fmt.Errorf("data too short for key length")
	}

	keyLen := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < 4+keyLen+4 {
		return Tx{}, fmt.Errorf("data too short for key and value length")
	}

	key := string(data[4 : 4+keyLen])

	valLen := binary.BigEndian.Uint32(data[4+keyLen : 4+keyLen+4])
	if uint32(len(data)) < 4+keyLen+4+valLen {
		return Tx{}, fmt.Errorf("data too short for value body")
	}

	value := make([]byte, valLen)
	copy(value, data[4+keyLen+4:4+keyLen+4+valLen])

	return Tx{Key: key, Value: value}, nil
}
