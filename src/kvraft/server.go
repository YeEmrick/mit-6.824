package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   Key
	Value Value
}

type Result struct {
	ErrorMsg  string
	ErrorCode ErrorCode
	Value     Value
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	resultCh map[int]chan Result // {logIdx: chan Result}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.ErrorCode = ErrWrongLeader
		return
	}
	op := Op{
		Type:  Get,
		Key:   args.Key,
		Value: "",
	}
	index, _, _ := kv.rf.Start(op)
	resCh := make(chan Result, 1)
	kv.resultCh[index] = resCh
	res := <-resCh
	reply.ErrorCode = res.ErrorCode
	reply.ErrorMsg = res.ErrorMsg
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Op != Put && args.Op != Append {
		reply.ErrorCode = UnsupportedOpType
		reply.ErrorMsg = fmt.Sprintf("Op should be Put(%v) or Append(%v)", Put, Append)
		return
	}
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.ErrorCode = ErrWrongLeader
		return
	}
	op := Op {
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	index, _, _ := kv.rf.Start(op)
	resCh := make(chan Result, 1)
	kv.resultCh[index] = resCh
	res := <-resCh
	reply.ErrorCode = res.ErrorCode
	reply.ErrorMsg = res.ErrorMsg
}

func (kv *KVServer) handleOp()  {
	for {
		applyResult := <-kv.applyCh
		op := applyResult.Command.(Op)
		switch op.Type {
		case Get:
			kv.handGet(applyResult)
			break
		case Put:
			kv.handPut(applyResult)
			break
		case Append:
			kv.handAppend(applyResult)
			break
		}
	}
}

func (kv *KVServer) handGet(applyResult raft.ApplyMsg)  {

}

func (kv *KVServer) handPut(applyResult raft.ApplyMsg)  {

}

func (kv *KVServer) handAppend(applyResult raft.ApplyMsg)  {

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
