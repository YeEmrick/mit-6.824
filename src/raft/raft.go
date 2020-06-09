package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"
import "labgob"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	FOLLOWER  int = 0
	CANDIDATE int = 1
	LEADER    int = 2
)

func getRoleString(role int) string {
	roles := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	return roles[role]
}

const (
	BEGIN_TERM = iota
	OVER_TERM
	STOP_TERM
	RESET_TERM
)

const (
	TERM_PERIOD_MIN  = time.Millisecond * 300
	TERM_PERIOD_MAX  = time.Millisecond * 600
	HEARTBEAT_PERIOD = time.Millisecond * 150
	RPC_TIMEOUT      = time.Millisecond * 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	rwmu        sync.RWMutex
	currentTern int
	votedFor    int
	log         []Entry
	role        int

	termTimer       *time.Timer
	heartbeatTimers []*time.Timer
	termAction      chan int

	// Volatile State
	commitIndex       int
	lastApplied       int
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// Reinitialized after termOver
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()

	return rf.currentTern, rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTern)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var CommitIndex int
	var LastApplied int
	var LastSnapshotIndex int
	var LastSnapshotTerm int
	var Log []Entry
	d.Decode(&CurrentTerm)
	d.Decode(&VotedFor)
	d.Decode(&CommitIndex)
	d.Decode(&LastApplied)
	d.Decode(&LastSnapshotIndex)
	d.Decode(&LastSnapshotTerm)
	d.Decode(&Log)
	rf.currentTern = CurrentTerm
	rf.votedFor = VotedFor
	rf.commitIndex = CommitIndex
	rf.lastApplied = LastApplied
	rf.lastSnapshotIndex = LastSnapshotIndex
	rf.lastSnapshotTerm = LastSnapshotTerm
	rf.log = Log
}

func (rf *Raft) up_to_date_or_eq(indexB int, termB int) bool {
	if rf.currentTern >= termB {
		return true
	}
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	if lastLogTerm != termB {
		return lastLogTerm > termB
	} else {
		return lastLogIndex >= indexB
	}
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return -1, rf.currentTern
	}
	term := rf.log[len(rf.log)-1].Term
	index := rf.lastSnapshotIndex + len(rf.log)
	return index, term
}

func (rf *Raft) getLocalLogIndex(idx int) int {
	return idx - rf.lastSnapshotIndex - 1
}

func (rf *Raft) getLogByIndex(idx int) (Entry, bool) {
	localIndex := rf.getLocalLogIndex(idx)
	if localIndex < 0 || localIndex >= len(rf.log) {
		return Entry{Index: -1, Term: 0}, false
	} else {
		return rf.log[localIndex], true
	}
}

func (rf *Raft) PrintNow(info string) {
	fmt.Printf("role:%v, Index:%v, Term:%v, info:%v\n", getRoleString(rf.role), rf.me, rf.currentTern, info)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an termOver. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runTerm() {
	rf.PrintNow("runTerm")
	go func() {
		for {
			<-rf.termTimer.C
			rf.PrintNow("OVER_TERM")
			rf.termAction <- OVER_TERM
		}
	}()
	for {
		select {
		case action := <-rf.termAction:
			switch action {
			case BEGIN_TERM:
				go rf.beginTerm()
			case OVER_TERM:
				go rf.endTerm()
			case RESET_TERM:
				rf.resetTermClick()
			case STOP_TERM:
				rf.stopTerm()
				return
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.rwmu = sync.RWMutex{}
	rf.currentTern = 0
	rf.role = FOLLOWER
	rf.termAction = make(chan int, 1)
	rf.persister = persister
	rf.log = make([]Entry, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	// init Timer
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.termTimer = time.NewTimer(rf.randTermTimeout())
	for i, _ := range rf.peers {
		rf.heartbeatTimers[i] = time.NewTimer(time.Duration(HEARTBEAT_PERIOD))
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.termAction <- BEGIN_TERM
	go rf.runTerm()

	return rf
}
