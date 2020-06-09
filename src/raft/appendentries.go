package raft

import "fmt"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// (1):leader认为prevLogIndex以及之前的所有log都与follower匹配了
	// 这一字段可以根据follower返回的nextIndex进行调整，以调整到(1)所述状态
	// 当prevLogIndex为(1)时，那么其后的所有entry将被commit到follower，
	// 此时如果follower中存在冲突，那么将被强行覆盖
	PrevLogIndex int
	PrevLogTerm  int

	//rf.log[PrevLogIndex+1:]
	Entries []Entry

	//follow通过此字段判断apply哪些entries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	rf.PrintNow(fmt.Sprintf("Receive entries from %v, Term: %v", args.LeaderId, args.Term))
	reply.Term = rf.currentTern
	if rf.currentTern > args.Term {
		reply.Success = false
		return
	}

	rf.currentTern = args.Term
	rf.changeTermTo(FOLLOWER)
	// heartbeat
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	// consistence check
	expectedPrevIndex, consistence := rf.consistenceCheck(args.PrevLogIndex, args.PrevLogTerm)
	if !consistence {
		reply.Success = false
		reply.NextIndex = expectedPrevIndex
	} else {
		reply.Success = true
		localPrevIndex := rf.getLocalLogIndex(args.PrevLogIndex)
		rf.log = append(rf.log[:localPrevIndex+1], args.Entries...)
		lastIndex := rf.log[len(rf.log)-1].Index
		reply.NextIndex = lastIndex
		if args.LeaderCommit > rf.commitIndex {
			if lastIndex < args.LeaderCommit {
				rf.commitIndex = lastIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
	}
	//rf.persist()
	return
}

func (rf *Raft) consistenceCheck(prevIndex int, prevTerm int) (int, bool) {
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	if prevIndex == -1 {
		// leader 第一次AppendEntries
		return prevIndex, true
	} else if prevIndex <= rf.lastSnapshotIndex {
		return rf.lastSnapshotIndex + 1, false
	} else if prevIndex > lastLogIndex {
		return lastLogIndex, false
	} else {
		entry, find := rf.getLogByIndex(prevIndex)
		if !find {
			return prevIndex - 1, false
		} else {
			if entry.Term == prevTerm {
				return prevIndex, true
			} else {
				return prevIndex - 1, true
			}
		}
	}
}

func (rf *Raft) appendEntries(peerIdx int) {
	rf.PrintNow(fmt.Sprintf("appendEntries to %v", peerIdx))
	var entries []Entry
	rf.rwmu.RLock()
	if rf.role != LEADER {
		rf.rwmu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peerIdx]
	prevLog, _ := rf.getLogByIndex(prevLogIndex)
	lastIndex, _ := rf.lastLogIndexAndTerm()
	for i := prevLogIndex + 1; i <= lastIndex; i++ {
		entry, find := rf.getLogByIndex(i)
		if !find {
			entries = []Entry{}
			break
		}
		entries = append(entries, entry)
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTern,
		LeaderId:     rf.me,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{
		Term:      rf.currentTern,
		Success:   true,
		NextIndex: 100,
	}
	rf.rwmu.RUnlock()
	rf.peers[peerIdx].Call("Raft.AppendEntries", &args, &reply)
	rf.handleAppendResponse(peerIdx, reply)
}

func (rf *Raft) handleAppendResponse(peerIdx int, reply AppendEntriesReply) {
	rf.nextIndex[peerIdx] = reply.NextIndex
	if !reply.Success {
		if reply.Term > rf.currentTern {
			rf.changeTermTo(FOLLOWER)
		}
	}
}
