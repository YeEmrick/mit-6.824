package raft

import "fmt"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.PrintNow(fmt.Sprintf("[RequestVote]Recive RequestVote from:%d, LastLogIndex:%d, LastLogTerm:%d", args.CandidateId, args.LastLogIndex, args.LastLogTerm))
	// Your code here (2A, 2B).
	rf.rwmu.Lock()

	// 检查Term
	reply.VoteGranted = false
	reply.Term = rf.currentTern
	if args.Term < rf.currentTern {
		rf.rwmu.Unlock()
		return
	}
	if args.Term == rf.currentTern {
		// leader不投票 || 已给其他候选人投票
		if rf.role == LEADER || rf.votedFor != args.CandidateId {
			rf.rwmu.Unlock()
			return
		}
	}
	reply.VoteGranted = rf.up_to_date(args.LastLogIndex, args.LastLogTerm) == false
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.currentTern = args.Term
		rf.changeTermTo(FOLLOWER)
	}
	rf.PrintNow(fmt.Sprintf("VoteGranted:%v, votedFor:%v", reply.VoteGranted, args.CandidateId))
	rf.persist()
	rf.rwmu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.PrintNow(fmt.Sprintf("sendRequestVote to:%d", server))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) up_to_date(indexB int, termB int) bool {
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	if lastLogTerm != termB {
		return lastLogTerm > termB
	} else {
		return lastLogIndex > indexB
	}
}
