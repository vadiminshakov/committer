package voteslog

import (
	"bytes"
	"encoding/gob"
	"github.com/pkg/errors"
	"io"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
)

// segmentInfoAndIndexMsg loads segment info (file descriptor, name, size, etc) and index from segment files for msgs log.
// Works like loadSegmentMsg, but for multiple segments.
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

// segmentInfoAndIndexVotes loads segment info (file descriptor, name, size, etc) and index from segment files for votes log.
// Works like loadSegmentVotes, but for multiple segments.
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

// loadSegment loads segment info (file descriptor, name, size, etc) and index from segment file.
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

// loadSegment loads segment info (file descriptor, name, size, etc) and index from segment file.
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

// findSegmentNumbers finds all segment numbers in the directory.
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

// loadIndexesMsg loads index from log file.
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

// loadIndexesMsg loads index from log file.
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
