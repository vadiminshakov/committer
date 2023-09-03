package voteslog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/core/entity"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	segmentThreshold        = 20
	tmpIndexBufferThreshold = 10
)

var ErrExists = errors.New("msg with such index already exists")

type FileVotesLog struct {
	// append-only log with proposed messages that node consumed
	msgs *os.File
	// append-only log with cohort votes on proposal
	votes *os.File
	// index that matches height of msg record with offset in file
	indexMsgs    map[uint64]msg
	tmpIndexMsgs map[uint64]msg
	// index that matches height of votes round record with offset in file
	indexVotes map[uint64]votesMsg

	encMsgs  *gob.Encoder
	encVotes *gob.Encoder
	bufMsgs  *bytes.Buffer
	bufVotes *bytes.Buffer

	lastOffsetMsgs  int64
	lastOffsetVotes int64

	pathToLogsDir string
}

func NewOnDiskLog(dir string) (*FileVotesLog, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return nil, errors.Wrap(err, "failed to create dir for votes log")
		}
	}
	msgs, err := os.OpenFile(dir+"/msgs_0", os.O_RDWR|os.O_CREATE, 0755)
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

	statVotes, err := votes.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read votes log file stat")
	}

	msgsIndex, err := loadIndexesMsg(msgs, statMsgs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load indexes from msg log file")
	}
	votesIndex, err := loadIndexesVotes(msgs, statMsgs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load indexes from votes log file")
	}

	var bufMsgs bytes.Buffer
	encMsgs := gob.NewEncoder(&bufMsgs)
	var bufVotes bytes.Buffer
	encVotes := gob.NewEncoder(&bufVotes)

	return &FileVotesLog{msgs: msgs, votes: votes, indexMsgs: msgsIndex, tmpIndexMsgs: make(map[uint64]msg), indexVotes: votesIndex, bufMsgs: &bufMsgs, bufVotes: &bufVotes,
		encMsgs: encMsgs, encVotes: encVotes, lastOffsetMsgs: statMsgs.Size(), lastOffsetVotes: statVotes.Size(), pathToLogsDir: dir}, nil
}

func loadIndexesMsg(file *os.File, stat os.FileInfo) (map[uint64]msg, error) {
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

func loadIndexesVotes(file *os.File, stat os.FileInfo) (map[uint64]votesMsg, error) {
	buf := make([]byte, stat.Size())
	if n, err := file.Read(buf); err != nil {
		if len(buf) == 0 && n == 0 && err == io.EOF {
			return make(map[uint64]votesMsg), nil
		} else if err != io.EOF {
			return nil, errors.Wrap(err, "failed to read log file")
		}
	}

	var msgs []votesMsg
	dec := gob.NewDecoder(bytes.NewReader(buf))
	for {
		var msgIndexed votesMsg
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

	index := make(map[uint64]votesMsg, len(msgs))
	for _, idxMsg := range msgs {
		index[idxMsg.Index] = idxMsg
	}

	return index, nil
}

func (c *FileVotesLog) Set(index uint64, key string, value []byte) error {
	if _, ok := c.indexMsgs[index]; ok {
		return ErrExists
	}

	if len(c.indexMsgs) == segmentThreshold {
		// rotate
		c.bufMsgs.Reset()
		c.encMsgs = gob.NewEncoder(c.bufMsgs)
		if err := c.msgs.Close(); err != nil {
			return errors.Wrap(err, "failed to close msgs log file")
		}

		_, suffix, ok := strings.Cut(c.msgs.Name(), "_")
		if !ok {
			return fmt.Errorf("failed to cut suffix from msgs log file name %s", c.msgs.Name())
		}
		i, err := strconv.Atoi(suffix)
		if err != nil {
			return fmt.Errorf("failed to convert suffix %s to int", suffix)
		}
		c.msgs, err = os.OpenFile(path.Join(c.pathToLogsDir, "msgs_"+strconv.Itoa(i+1)), os.O_RDWR|os.O_CREATE, 0755)
		if err = os.Remove(c.msgs.Name()); err != nil {
			return errors.Wrapf(err, "failed to remove old segment %s", c.msgs.Name())
		}
		c.lastOffsetMsgs = 0
	}

	// gob encode key and value
	if err := c.encMsgs.Encode(msg{index, key, value}); err != nil {
		return errors.Wrap(err, "failed to encode msg for log")
	}
	// write to log at last offset
	_, err := c.msgs.WriteAt(c.bufMsgs.Bytes(), c.lastOffsetMsgs)
	if err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}
	if err := c.msgs.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync msg log file")
	}

	c.lastOffsetMsgs += int64(c.bufMsgs.Len())
	c.bufMsgs.Reset()
	// update index
	c.indexMsgs[index] = msg{index, key, value}
	if len(c.indexMsgs) > segmentThreshold {
		if len(c.tmpIndexMsgs) == tmpIndexBufferThreshold {
			c.indexMsgs = c.tmpIndexMsgs
			c.tmpIndexMsgs = make(map[uint64]msg)
			return nil
		}
		c.tmpIndexMsgs[index] = msg{index, key, value}
	}
	return nil
}

func (c *FileVotesLog) Get(index uint64) (string, []byte, bool) {
	msg, ok := c.indexMsgs[index]
	if !ok {
		return "", nil, false
	}

	return msg.Key, msg.Value, true
}

func (c *FileVotesLog) SetVotes(index uint64, votes []*entity.Vote) error {
	if _, ok := c.indexVotes[index]; ok {
		return ErrExists
	}
	// gob encode key and value
	if err := c.encVotes.Encode(votesMsg{index, votes}); err != nil {
		return errors.Wrap(err, "failed to encode msg for log")
	}
	// write to log at last offset
	_, err := c.votes.WriteAt(c.bufVotes.Bytes(), c.lastOffsetVotes)
	if err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}
	if err := c.votes.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync votes log file")
	}

	c.lastOffsetVotes += int64(c.bufVotes.Len())
	c.bufVotes.Reset()
	// update index
	c.indexVotes[index] = votesMsg{index, votes}
	return nil
}

func (c *FileVotesLog) GetVotes(index uint64) []*entity.Vote {
	msg, ok := c.indexVotes[index]
	if !ok {
		return nil
	}

	return msg.Votes
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
