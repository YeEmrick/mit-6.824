package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"time"
)

func (rf *Raft) PrintNow(info string) {
	fmt.Printf("role:%v, Index:%v, Term:%v, LogLen:%v, CommitIndex:%v, AppliedIndex:%v, info:%v\n", getRoleString(rf.role), rf.me, rf.currentTern, len(rf.log), rf.commitIndex, rf.lastAppliedIndex, info)
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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
	err := e.Encode(rf.currentTern)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	err = e.Encode(rf.commitIndex)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	err = e.Encode(rf.lastAppliedIndex)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	err = e.Encode(rf.lastSnapshotIndex)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	err = e.Encode(rf.lastSnapshotTerm)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	err = e.Encode(rf.log)
	if err != nil {
		rf.PrintNow(fmt.Sprint(err))
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
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
	rf.lastAppliedIndex = LastApplied
	rf.lastSnapshotIndex = LastSnapshotIndex
	rf.lastSnapshotTerm = LastSnapshotTerm
	rf.log = Log
}

func (rf *Raft) call(peerIdx int, funcName string, args *interface{}, reply *interface{}) {
	rpcTimer := time.NewTimer(RPC_TIMEOUT)
	for {
		rpcCh := make(chan bool, 1)
		go func(peerIdx int, funcName string, args *interface{}, reply *interface{}) {
			defer func() {
				recover()
			}()
			rpcCh <- rf.peers[peerIdx].Call(funcName, args, reply)
		}(peerIdx, funcName, args, reply)

		select {
		case <-rpcTimer.C: //Rpc Timeout
			rpcTimer.Reset(RPC_TIMEOUT)
		case ok := <-rpcCh:
			if ok {
				break
			} else {
				time.Sleep(2 * RPC_TIMEOUT)
			}
		}
		close(rpcCh)
	}
}
