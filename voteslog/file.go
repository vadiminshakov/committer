package voteslog

import (
	"bytes"
	"encoding/gob"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/core/entity"
	"io"
	"os"
)

type FileVotesLog struct {
	// append-only log with proposed messages that node consumed
	msgs *os.File
	// append-only log with cohort votes on proposal
	votes *os.File
	// index that matches height of msg record with offset in file
	indexMsgs map[uint64]msg
	// index that matches height of votes round record with offset in file
	indexVotes map[uint64][]entity.Vote

	enc *gob.Encoder
	buf *bytes.Buffer

	lastOffsetMsgs  int64
	lastOffsetVotes int64
}

func NewOnDiskLog(dir string) (*FileVotesLog, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return nil, errors.Wrap(err, "failed to create dir for votes log")
		}
	}
	msgs, err := os.OpenFile(dir+"/msgs", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open msg log file")
	}
	statMsgs, err := msgs.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read msg log file stat")
	}

	votes, err := os.OpenFile(dir+"/votes", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open votes log file")
	}

	statVotes, err := msgs.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read votes log file stat")
	}
	_ = statVotes

	msgsIndex, err := loadIndexes(msgs, statMsgs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load indexes from msg log file")
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	return &FileVotesLog{msgs: msgs, votes: votes, indexMsgs: msgsIndex, buf: &buf, enc: enc,
		lastOffsetMsgs: statMsgs.Size(), lastOffsetVotes: statVotes.Size()}, nil
}

func loadIndexes(file *os.File, stat os.FileInfo) (map[uint64]msg, error) {
	buf := make([]byte, stat.Size())
	if n, err := file.Read(buf); err != nil {
		if len(buf) == 0 && n == 0 && err == io.EOF {
			return make(map[uint64]msg), nil
		} else if err != io.EOF {
			return nil, errors.Wrap(err, "failed to read log file")
		}
	}

	var msgs []msg
	dec := gob.NewDecoder(bytes.NewReader(buf))
	for {
		var msgIndexed msg
		if err := dec.Decode(&msgIndexed); err != nil {
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, errors.Wrap(err, "failed to decode indexed msg from log")
			}
		}
		msgs = append(msgs, msgIndexed)
	}

	index := make(map[uint64]msg, len(msgs))
	for _, idxMsg := range msgs {
		index[idxMsg.Index] = idxMsg
	}

	return index, nil
}

func (c *FileVotesLog) Set(index uint64, key string, value []byte) error {
	// gob encode key and value
	if err := c.enc.Encode(msg{index, key, value}); err != nil {
		return errors.Wrap(err, "failed to encode msg for log")
	}
	// write to log at last offset
	_, err := c.msgs.WriteAt(c.buf.Bytes(), c.lastOffsetMsgs)
	if err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}
	c.lastOffsetVotes += int64(c.buf.Len())
	// update index
	c.indexMsgs[index] = msg{index, key, value}
	return nil
}

func (c *FileVotesLog) Get(index uint64) (string, []byte, bool) {
	msg, ok := c.indexMsgs[index]
	if !ok {
		return "", nil, false
	}

	return msg.Key, msg.Value, true
}

func (c *FileVotesLog) Delete(index uint64) {
}

func (c *FileVotesLog) SetVotes(index uint64, votes []*entity.Vote) {
	//c.muVotes.Lock()
	//defer c.muVotes.Unlock()
	//c.votes[index] = append(c.votes[index], votes...)
}

func (c *FileVotesLog) GetVotes(index uint64) []*entity.Vote {
	//c.muVotes.RLock()
	//defer c.muVotes.RUnlock()
	//return c.votes[index]
	return nil
}

func (c *FileVotesLog) DelVotes(index uint64) {
	//c.muVotes.Lock()
	//delete(c.votes, index)
	//c.muVotes.Unlock()
}

func (c *FileVotesLog) Close() error {
	if err := c.msgs.Close(); err != nil {
		return errors.Wrap(err, "failed to close msgs log file")
	}
	if err := c.votes.Close(); err != nil {
		return errors.Wrap(err, "failed to close votes log file")
	}
	return nil
}
