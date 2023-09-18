package voteslog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/core/entity"
	"io"
	"maps"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

const (
	segmentThreshold        = 1000
	tmpIndexBufferThreshold = 500
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

	// gob encoder for proposed messages
	encMsgs *gob.Encoder
	// gob encoder for cohort votes
	encVotes *gob.Encoder

	// buffer for proposed messages
	bufMsgs *bytes.Buffer
	// buffer for cohort votes
	bufVotes *bytes.Buffer

	// offset of last msg record in file
	lastOffsetMsgs int64
	// offset of last votes round record in file
	lastOffsetVotes int64

	// path to directory with logs
	pathToLogsDir string

	// name of the old segment for msgs log
	oldMsgsSegmentName string
}

func NewOnDiskLog(dir string) (*FileVotesLog, error) {
	msgSegmentsNumbers, voteSegmentsNumbers, err := findSegmentNumbers(dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	// load them segments into mem
	msgs, statMsgs, msgsIndex, err := segmentInfoAndIndexMsg(msgSegmentsNumbers, path.Join(dir, "msgs_"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load msgs segments")
	}

	votes, statVotes, votesIndex, err := segmentInfoAndIndexVotes(voteSegmentsNumbers, path.Join(dir, "votes_"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load votes segments")
	}

	var bufMsgs bytes.Buffer
	encMsgs := gob.NewEncoder(&bufMsgs)
	var bufVotes bytes.Buffer
	encVotes := gob.NewEncoder(&bufVotes)

	return &FileVotesLog{msgs: msgs, votes: votes, indexMsgs: msgsIndex, tmpIndexMsgs: make(map[uint64]msg), indexVotes: votesIndex, bufMsgs: &bufMsgs, bufVotes: &bufVotes,
		encMsgs: encMsgs, encVotes: encVotes, lastOffsetMsgs: statMsgs.Size(), lastOffsetVotes: statVotes.Size(), pathToLogsDir: dir}, nil
}

func segmentInfoAndIndexMsg(segNumbers []int, path string) (*os.File, os.FileInfo, map[uint64]msg, error) {
	index := make(map[uint64]msg)
	var (
		logFileFD      *os.File
		logFileInfo    os.FileInfo
		idxFromSegment map[uint64]msg
		err            error
	)
	for _, segindex := range segNumbers {
		logFileFD, logFileInfo, idxFromSegment, err = loadSegmentMsg(path + strconv.Itoa(segindex))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to load indexes from msg log file")
		}

		maps.Copy(index, idxFromSegment)
	}

	return logFileFD, logFileInfo, index, nil
}

func segmentInfoAndIndexVotes(segNumbers []int, path string) (*os.File, os.FileInfo, map[uint64]votesMsg, error) {
	index := make(map[uint64]votesMsg)
	var (
		logFileFD      *os.File
		logFileInfo    os.FileInfo
		idxFromSegment map[uint64]votesMsg
		err            error
	)
	for _, segindex := range segNumbers {
		logFileFD, logFileInfo, idxFromSegment, err = loadSegmentVote(path + strconv.Itoa(segindex))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to load indexes from msg log file")
		}

		maps.Copy(index, idxFromSegment)
	}

	return logFileFD, logFileInfo, index, nil
}

func loadSegmentMsg(path string) (fd *os.File, fileinfo os.FileInfo, index map[uint64]msg, err error) {
	fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to open log segment file")
	}

	fileinfo, err = fd.Stat()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read log segment file stat")
	}

	index, err = loadIndexesMsg(fd, fileinfo)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to build index from log segment")
	}

	return fd, fileinfo, index, nil
}

func loadSegmentVote(path string) (fd *os.File, fileinfo os.FileInfo, index map[uint64]votesMsg, err error) {
	fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to open log segment file")
	}

	fileinfo, err = fd.Stat()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read log segment file stat")
	}

	index, err = loadIndexesVotes(fd, fileinfo)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to build index from log segment")
	}

	return fd, fileinfo, index, nil
}

func findSegmentNumbers(dir string) (msgSegmentsNumbers []int, voteSegmentsNumbers []int, err error) {
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return nil, nil, errors.Wrap(err, "failed to create dir for votes log")
		}
	}
	de, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to read dir for votes log")
	}

	msgSegmentsNumbers = make([]int, 0)
	voteSegmentsNumbers = make([]int, 0)
	for _, d := range de {
		if d.IsDir() {
			continue
		}
		if strings.HasPrefix(d.Name(), "msgs_") {
			i, err := extractSegmentNum(d.Name())
			if err != nil {
				return nil, nil, errors.Wrap(err, "initialization failed: failed to extract segment number from msgs log file name")
			}

			msgSegmentsNumbers = append(msgSegmentsNumbers, i)
		}

		if strings.HasPrefix(d.Name(), "votes_") {
			i, err := extractSegmentNum(d.Name())
			if err != nil {
				return nil, nil, errors.Wrap(err, "initialization failed: failed to extract segment number from votes log file name")
			}

			voteSegmentsNumbers = append(voteSegmentsNumbers, i)
		}
	}

	sort.Slice(msgSegmentsNumbers, func(i, j int) bool {
		return msgSegmentsNumbers[i] < msgSegmentsNumbers[j]
	})
	sort.Slice(voteSegmentsNumbers, func(i, j int) bool {
		return voteSegmentsNumbers[i] < voteSegmentsNumbers[j]
	})

	if len(msgSegmentsNumbers) == 0 {
		msgSegmentsNumbers = append(msgSegmentsNumbers, 0)
	}
	if len(voteSegmentsNumbers) == 0 {
		voteSegmentsNumbers = append(voteSegmentsNumbers, 0)
	}

	return msgSegmentsNumbers, voteSegmentsNumbers, nil
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
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to decode indexed msg from log")
		}
		msgs = append(msgs, msgIndexed)
	}

	index := make(map[uint64]msg, len(msgs))
	for _, idxMsg := range msgs {
		index[idxMsg.Idx] = idxMsg
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
		index[idxMsg.Idx] = idxMsg
	}

	return index, nil
}

func (c *FileVotesLog) Set(index uint64, key string, value []byte) error {
	if _, ok := c.indexMsgs[index]; ok {
		return ErrExists
	}

	// rotate segment if threshold is reached
	// (close current segment, open new one with incremented suffix in name, remove old segment)
	if len(c.indexMsgs) == segmentThreshold {
		c.bufMsgs.Reset()
		c.encMsgs = gob.NewEncoder(c.bufMsgs)
		if err := c.msgs.Close(); err != nil {
			return errors.Wrap(err, "failed to close msgs log file")
		}
		c.oldMsgsSegmentName = c.msgs.Name()

		segmentIndex, err := extractSegmentNum(c.msgs.Name())
		if err != nil {
			return errors.Wrap(err, "failed to extract segment number from msgs log file name")
		}

		c.msgs, err = os.OpenFile(path.Join(c.pathToLogsDir, "msgs_"+strconv.Itoa(segmentIndex+1)), os.O_RDWR|os.O_CREATE, 0755)

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

	// if threshold is reached, start writing to tmp index buffer
	if len(c.indexMsgs) > segmentThreshold {
		// if tmp index buffer is full, flush it to index and rm old segment and associated index file
		if len(c.tmpIndexMsgs) == tmpIndexBufferThreshold {
			c.indexMsgs = c.tmpIndexMsgs
			c.tmpIndexMsgs = make(map[uint64]msg)

			if err = os.Remove(c.oldMsgsSegmentName); err != nil {
				return errors.Wrapf(err, "failed to remove old segment %s", c.msgs.Name())
			}
			return nil
		}
		c.tmpIndexMsgs[index] = msg{index, key, value}
	}
	return nil
}

func extractSegmentNum(segmentName string) (int, error) {
	_, suffix, ok := strings.Cut(segmentName, "_")
	if !ok {
		return 0, fmt.Errorf("failed to cut suffix from msgs log file name %s", segmentName)
	}
	i, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("failed to convert suffix %s to int", suffix)
	}

	return i, nil
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
