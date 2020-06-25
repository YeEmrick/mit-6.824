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

	//rf.log[LeaderCommit:PrevLogIndex]
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
	rf.PrintNow(fmt.Sprintf("May Receive entries from %v, Term: %v, Commit: %v", args.LeaderId, args.Term, args.LeaderCommit))
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	reply.Term = rf.currentTern
	if rf.currentTern > args.Term {
		reply.Success = false
		return
	}
	// 同步信息
	rf.currentTern = args.Term
	lastIndex, _ := rf.lastLogIndexAndTerm()
	if args.LeaderCommit > rf.commitIndex {
		if lastIndex < args.LeaderCommit {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// heartbeat
	rf.changeTermTo(FOLLOWER)
	if len(args.Entries) == 0 {
		reply.Success = true
		rf.PrintNow(fmt.Sprintf("Receive Heartbeats from %v, Term: %v", args.LeaderId, args.Term))
		return
	}

	// consistence check
	rf.PrintNow(fmt.Sprintf("Receive entries from %v, Term: %v", args.LeaderId, args.Term))
	expectedNextIndex, consistence := rf.consistenceCheck(args.PrevLogIndex, args.PrevLogTerm)
	if !consistence {
		reply.Success = false
		reply.NextIndex = expectedNextIndex
	} else {
		rf.PrintNow(fmt.Sprintf("Receive valid entries from %v, Term: %v, PrevLogIndex：%v", args.LeaderId, args.Term, args.PrevLogIndex))
		reply.Success = true
		localPrevIndex := rf.getLocalLogIndex(args.PrevLogIndex)
		rf.log = append(rf.log[localPrevIndex+1:], args.Entries...)
		lastIndex, _ := rf.lastLogIndexAndTerm()
		reply.NextIndex = lastIndex + 1
		rf.nextIndex[rf.me] = lastIndex + 1
		rf.matchIndex[rf.me] = lastIndex
		rf.PrintNow(fmt.Sprintf("After Receive valid entries from %v, Term: %v, LogLength: %v", args.LeaderId, args.Term, len(rf.log)))
	}
	//rf.persist()
	return
}

func (rf *Raft) consistenceCheck(prevIndex int, prevTerm int) (int, bool) {
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	if prevIndex == -1 {
		// leader 第一次AppendEntries
		return prevIndex + 1, true
	} else if prevIndex <= rf.lastSnapshotIndex {
		return rf.lastSnapshotIndex + 1, false
	} else if prevIndex > lastLogIndex {
		return lastLogIndex + 1, false
	} else {
		entry, find := rf.getLogByIndex(prevIndex)
		if find && entry.Term == prevTerm {
			return prevIndex + 1, true
		} else {
			return prevIndex, false
		}
	}
}

func (rf *Raft) appendEntries(peerIdx int) {
	rf.PrintNow(fmt.Sprintf("[BEGIN]appendEntries to %v", peerIdx))
	var entries []Entry
	rf.rwmu.RLock()
	if rf.role != LEADER {
		rf.rwmu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peerIdx] - 1
	prevLog, _ := rf.getLogByIndex(prevLogIndex)
	lastIndex, _ := rf.lastLogIndexAndTerm()
	for i := prevLogIndex + 1; i <= lastIndex; i++ {
		entry, find := rf.getLogByIndex(i)
		if !find {
			entries = []Entry{}
		} else {
			entries = append(entries, entry)
		}
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTern,
		LeaderId:     rf.me,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.rwmu.RUnlock()
	rf.peers[peerIdx].Call("Raft.AppendEntries", &args, &reply)

	// handle response
	rf.rwmu.Lock()
	if reply.Success {
		if reply.NextIndex > rf.nextIndex[peerIdx] {
			rf.nextIndex[peerIdx] = reply.NextIndex
			rf.matchIndex[peerIdx] = reply.NextIndex - 1
		}
		_, lastTerm := rf.lastLogIndexAndTerm()
		// Paper 5.4 safety：只有最后log的term是当前term时，才commit
		if lastTerm == rf.currentTern {
			rf.updateCommitIndex()
		}
	} else {
		if reply.NextIndex > rf.lastSnapshotIndex {
			rf.nextIndex[peerIdx] = reply.NextIndex
		}
		if reply.Term > rf.currentTern {
			rf.changeTermTo(FOLLOWER)
		}
	}
	rf.rwmu.Unlock()
}

func (rf *Raft) updateCommitIndex() {
	lastIndex, _ := rf.lastLogIndexAndTerm()
	for i := rf.commitIndex + 1; i <= lastIndex; i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count++
			}
		}
		if count >= len(rf.peers)/2 {
			rf.commitIndex = i
		} else {
			break
		}
	}
	//todo:触发apply操作
}
