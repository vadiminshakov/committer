package voteslog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/core/entity"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	segmentThreshold = 1000
)

var ErrExists = errors.New("msg with such index already exists")

const maxSegments = 5

// FileVotesLog is used to store votes and msgs on disk.
//
// Log is append-only, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
// Log is divided into two parts: msgs log and votes log. Each part has its own index, which is used to find record by its height.
// Index is stored in memory and loaded from disk on startup.
//
// This code is intentionally monomorphized for msgs and votes, generics can slow the app and make code more complicated.
type FileVotesLog struct {
	// append-only log with proposed messages that node consumed
	msgs *os.File
	// append-only log with cohort votes on proposal
	votes *os.File
	// index that matches height of msg record with offset in file
	indexMsgs    map[uint64]msg
	tmpIndexMsgs map[uint64]msg
	// index that matches height of votes round record with offset in file
	indexVotes    map[uint64]votesMsg
	tmpIndexVotes map[uint64]votesMsg

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
	oldestMsgsSegmentName string
	// name of the old segment for votes log
	oldestVotesSegmentName string

	// number of segments for msgs log
	segmentsNumberMsgs int
	// number of segments for votes log
	segmentsNumberVotes int
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
	numberOfMsgsSegments := len(msgsIndex) / segmentThreshold
	if numberOfMsgsSegments == 0 {
		numberOfMsgsSegments = 1
	}

	votes, statVotes, votesIndex, err := segmentInfoAndIndexVotes(voteSegmentsNumbers, path.Join(dir, "votes_"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load votes segments")
	}
	numberOfVotesSegments := len(votesIndex) / segmentThreshold
	if numberOfVotesSegments == 0 {
		numberOfVotesSegments = 1
	}

	var bufMsgs bytes.Buffer
	encMsgs := gob.NewEncoder(&bufMsgs)
	var bufVotes bytes.Buffer
	encVotes := gob.NewEncoder(&bufVotes)

	return &FileVotesLog{msgs: msgs, votes: votes, indexMsgs: msgsIndex, tmpIndexMsgs: make(map[uint64]msg),
		tmpIndexVotes: make(map[uint64]votesMsg), indexVotes: votesIndex, bufMsgs: &bufMsgs, bufVotes: &bufVotes,
		encMsgs: encMsgs, encVotes: encVotes, lastOffsetMsgs: statMsgs.Size(), lastOffsetVotes: statVotes.Size(), pathToLogsDir: dir,
		segmentsNumberMsgs: numberOfMsgsSegments, segmentsNumberVotes: numberOfVotesSegments}, nil
}

// Set writes key/value pair to the msgs log.
func (c *FileVotesLog) Set(index uint64, key string, value []byte) error {
	if _, ok := c.indexMsgs[index]; ok {
		return ErrExists
	}

	// rotate segment if threshold is reached
	// (close current segment, open new one with incremented suffix in name)
	itemsAddedTotal := len(c.indexMsgs)
	if itemsAddedTotal > maxSegments*segmentThreshold {
		itemsAddedTotal = itemsAddedTotal + (c.segmentsNumberMsgs-1)*segmentThreshold
	}
	if itemsAddedTotal == segmentThreshold*c.segmentsNumberMsgs {
		c.bufMsgs.Reset()
		c.encMsgs = gob.NewEncoder(c.bufMsgs)
		if err := c.msgs.Close(); err != nil {
			return errors.Wrap(err, "failed to close msgs log file")
		}

		c.oldestMsgsSegmentName = c.oldestSegmentName(c.segmentsNumberMsgs)

		segmentIndex, err := extractSegmentNum(c.msgs.Name())
		if err != nil {
			return errors.Wrap(err, "failed to extract segment number from msgs log file name")
		}

		segmentIndex++
		c.msgs, err = os.OpenFile(path.Join(c.pathToLogsDir, "msgs_"+strconv.Itoa(segmentIndex)), os.O_RDWR|os.O_CREATE, 0755)
		c.segmentsNumberMsgs = segmentIndex + 1

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
	//if err := c.msgs.Sync(); err != nil {
	//	return errors.Wrap(err, "failed to sync msg log file")
	//}

	c.lastOffsetMsgs += int64(c.bufMsgs.Len())
	c.bufMsgs.Reset()

	// update index
	c.indexMsgs[index] = msg{index, key, value}

	c.rotateSegmentMsgs(msg{index, key, value})

	return nil
}

// oldestSegmentName returns name of the oldest segment in the directory.
func (c *FileVotesLog) oldestSegmentName(numberOfSegments int) string {
	latestSegmentIndex := 0
	if numberOfSegments >= maxSegments {
		latestSegmentIndex = numberOfSegments - maxSegments
	}
	return path.Join(c.pathToLogsDir, "msgs_"+strconv.Itoa(int(latestSegmentIndex)))
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

// Get queries value at specific index in the msgs log.
func (c *FileVotesLog) Get(index uint64) (string, []byte, bool) {
	msg, ok := c.indexMsgs[index]
	if !ok {
		return "", nil, false
	}

	return msg.Key, msg.Value, true
}

// SetVotes writes cohort votes to the votes log.
func (c *FileVotesLog) SetVotes(index uint64, votes []*entity.Vote) error {
	if _, ok := c.indexVotes[index]; ok {
		return ErrExists
	}

	// rotate segment if threshold is reached
	// (close current segment, open new one with incremented suffix in name)
	itemsAddedTotal := len(c.indexMsgs)
	if itemsAddedTotal > maxSegments*segmentThreshold {
		itemsAddedTotal = itemsAddedTotal + (c.segmentsNumberVotes-1)*segmentThreshold
	}
	if itemsAddedTotal == segmentThreshold*c.segmentsNumberVotes {
		c.bufVotes.Reset()
		c.encVotes = gob.NewEncoder(c.bufVotes)
		if err := c.votes.Close(); err != nil {
			return errors.Wrap(err, "failed to close votes log file")
		}

		c.oldestVotesSegmentName = c.oldestSegmentName(c.segmentsNumberVotes)

		segmentIndex, err := extractSegmentNum(c.votes.Name())
		if err != nil {
			return errors.Wrap(err, "failed to extract segment number from votes log file name")
		}

		segmentIndex++
		c.votes, err = os.OpenFile(path.Join(c.pathToLogsDir, "votes_"+strconv.Itoa(segmentIndex)), os.O_RDWR|os.O_CREATE, 0755)
		c.segmentsNumberVotes = segmentIndex + 1

		c.lastOffsetVotes = 0
	}

	// gob encode key and value
	if err := c.encVotes.Encode(votesMsg{index, votes}); err != nil {
		return errors.Wrap(err, "failed to encode msg for votes log")
	}
	// write to log at last offset
	_, err := c.votes.WriteAt(c.bufVotes.Bytes(), c.lastOffsetVotes)
	if err != nil {
		return errors.Wrap(err, "failed to write msg to votes log")
	}
	//if err := c.votes.Sync(); err != nil {
	//	return errors.Wrap(err, "failed to sync votes log file")
	//}

	c.lastOffsetVotes += int64(c.bufVotes.Len())
	c.bufVotes.Reset()

	// update index
	c.indexVotes[index] = votesMsg{index, votes}

	c.rotateSegmentVotes(votesMsg{index, votes})

	return nil
}

// GetVotes queries cohort votes at specific index in the votes log.
func (c *FileVotesLog) GetVotes(index uint64) []*entity.Vote {
	msg, ok := c.indexVotes[index]
	if !ok {
		return nil
	}

	return msg.Votes
}

// Close closes log files.
func (c *FileVotesLog) Close() error {
	if err := c.msgs.Close(); err != nil {
		return errors.Wrap(err, "failed to close msgs log file")
	}
	if err := c.votes.Close(); err != nil {
		return errors.Wrap(err, "failed to close votes log file")
	}
	return nil
}
