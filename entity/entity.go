package entity

type ProposeRequest struct {
	Key    string
	Value  []byte
	Height uint64
}

type CommitRequest struct {
	Height     uint64
	IsRollback bool
}

type ResponseType int32

const (
	ResponseTypeAck ResponseType = iota
	ResponseTypeNack
)

type Response struct {
	ResponseType
	Height uint64
}

type Vote struct {
	Node       string
	IsAccepted bool
}

const (
	ResponseAck = iota
	ResponseNack
)

type BroadcastRequest struct {
	Key   string
	Value []byte
}

type BroadcastResponse struct {
	Type  int32
	Index uint64
}
