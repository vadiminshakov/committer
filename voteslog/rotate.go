package voteslog

import (
	"log"
	"os"
)

// rotateSegmentMsgs rotates msgs segment if threshold is reached.
// If number of items in index file exceeds threshold, start writing to tmp index buffer.
func (c *FileVotesLog) rotateSegmentMsgs(newMsg msg) {
	multiplier := 1
	if c.segmentsNumberMsgs > 1 {
		multiplier = c.segmentsNumberMsgs - 1
	}
	// if threshold is reached, start writing to tmp index buffer
	if len(c.indexMsgs) > segmentThreshold*multiplier {
		// if segments number exceeds the limit, flush tmp index to main index and rm oldest segment
		if c.segmentsNumberMsgs > maxSegments {
			c.indexMsgs = c.tmpIndexMsgs
			c.tmpIndexMsgs = make(map[uint64]msg)

			go func(segmentName string) {
				if err := os.Remove(segmentName); err != nil {
					log.Printf("failed to remove oldest segment %s, error: %s", segmentName, err)
				}
			}(c.oldestMsgsSegmentName)
			return
		}
		c.tmpIndexMsgs[newMsg.Index()] = newMsg
	}

	return
}

// rotateSegmentVotes rotates votes segment if threshold is reached.
// If number of items in index file exceeds threshold, start writing to tmp index buffer.
func (c *FileVotesLog) rotateSegmentVotes(newMsg votesMsg) {
	multiplier := 1
	if c.segmentsNumberVotes > 1 {
		multiplier = c.segmentsNumberVotes - 1
	}
	// if threshold is reached, start writing to tmp index buffer
	if len(c.indexVotes) > segmentThreshold*multiplier {
		// if segments number exceeds the limit, flush tmp index to main index and rm oldest segment
		if c.segmentsNumberVotes > maxSegments {
			c.indexVotes = c.tmpIndexVotes
			c.tmpIndexVotes = make(map[uint64]votesMsg)

			go func(segmentName string) {
				if err := os.Remove(segmentName); err != nil {
					log.Printf("failed to remove oldest segment %s, error: %s", segmentName, err)
				}
			}(c.oldestVotesSegmentName)
			return
		}
		c.tmpIndexVotes[newMsg.Index()] = newMsg
	}

	return
}
