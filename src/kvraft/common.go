package kvraft

// Error Code
const (
	OK = iota
	ErrNoKey
	ErrWrongLeader
)

// Op
const (
	Put = iota
	Get
	Append
)

type ErrCode int

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err ErrCode
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err    ErrCode
	ErrMsg string
	Value  string
}
