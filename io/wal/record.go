package wal

import (
	"encoding/binary"
	"errors"
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

// lengthPrefixSize is the size in bytes of the length prefixes framing keys
// and values in the WAL record encoding.
const lengthPrefixSize = 4

func PreparedKey(height uint64) string {
	return txPrefix + "prepared:" + strconv.FormatUint(height, 10)
}
func PrecommitKey(height uint64) string {
	return txPrefix + "precommit:" + strconv.FormatUint(height, 10)
}
func CommitKey(height uint64) string { return txPrefix + "commit:" + strconv.FormatUint(height, 10) }
func AbortKey(height uint64) string  { return txPrefix + "abort:" + strconv.FormatUint(height, 10) }

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

// Encode serializes a transaction into bytes.
// Format: [KeyLen(4 bytes)] [KeyBytes] [ValueLen(4 bytes)] [ValueBytes].
func Encode(transaction Tx) ([]byte, error) {
	keyLen := uint32(len(transaction.Key))
	valLen := uint32(len(transaction.Value))

	buf := make([]byte, lengthPrefixSize+keyLen+lengthPrefixSize+valLen)

	binary.BigEndian.PutUint32(buf[0:lengthPrefixSize], keyLen)
	copy(buf[lengthPrefixSize:lengthPrefixSize+keyLen], transaction.Key)

	binary.BigEndian.PutUint32(buf[lengthPrefixSize+keyLen:lengthPrefixSize+keyLen+lengthPrefixSize], valLen)
	copy(buf[lengthPrefixSize+keyLen+lengthPrefixSize:], transaction.Value)

	return buf, nil
}

// Decode deserializes bytes into a Tx.
func Decode(data []byte) (Tx, error) {
	if len(data) < lengthPrefixSize {
		return Tx{}, errors.New("data too short for key length")
	}

	keyLen := binary.BigEndian.Uint32(data[0:lengthPrefixSize])
	if uint32(len(data)) < lengthPrefixSize+keyLen+lengthPrefixSize {
		return Tx{}, errors.New("data too short for key and value length")
	}

	key := string(data[lengthPrefixSize : lengthPrefixSize+keyLen])

	valLen := binary.BigEndian.Uint32(data[lengthPrefixSize+keyLen : lengthPrefixSize+keyLen+lengthPrefixSize])
	if uint32(len(data)) < lengthPrefixSize+keyLen+lengthPrefixSize+valLen {
		return Tx{}, errors.New("data too short for value body")
	}

	value := make([]byte, valLen)
	copy(value, data[lengthPrefixSize+keyLen+lengthPrefixSize:lengthPrefixSize+keyLen+lengthPrefixSize+valLen])

	return Tx{Key: key, Value: value}, nil
}
