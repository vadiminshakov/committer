package voteslog

type msg struct {
	Index uint64
	Key   string
	Value []byte
}
