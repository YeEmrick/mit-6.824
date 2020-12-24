package kvraft

// Error Code
const (
	OK = iota
	ErrNoKey
	ErrWrongLeader
	UnsupportedOpType
)

// Op
const (
	Put = iota
	Get
	Append
)

type ErrorCode 	int
type OpType		int
type Key		string
type Value		string

// Put or Append
type PutAppendArgs struct {
	Key   Key
	Value Value
	Op    OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	ErrorCode ErrorCode
	ErrorMsg  string
}

type GetArgs struct {
	Key Key
	// You'll have to add definitions here.
}

type GetReply struct {
	ErrorCode ErrorCode
	ErrorMsg  string
	Value     Value
}
