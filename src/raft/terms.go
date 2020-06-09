package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) beginTerm() {
	rf.PrintNow("beginTerm")
	switch rf.role {
	case CANDIDATE:
		rf.candidateTerm()
	case LEADER:
		rf.leaderTerm()
	case FOLLOWER:
		rf.followerTerm()
	}
}

func (rf *Raft) changeTermTo(role int) {
	rf.PrintNow(fmt.Sprintf("changeTermTo:%v", getRoleString(role)))
	rf.role = role
	rf.resetTermClick()
	rf.termAction <- BEGIN_TERM
}

func (rf *Raft) endTerm() {
	rf.rwmu.Lock()
	rf.PrintNow("endTerm")
	switch rf.role {
	case FOLLOWER:
		rf.role = CANDIDATE
	default:
		rf.resetTermClick()
	}

	rf.rwmu.Unlock()
	rf.termAction <- BEGIN_TERM
}

func (rf *Raft) stopTerm() {

}

func (rf *Raft) randTermTimeout() time.Duration {
	return time.Duration(rand.Int63())%(TERM_PERIOD_MAX-TERM_PERIOD_MIN) + TERM_PERIOD_MIN
}

func (rf *Raft) candidateTerm() {
	rf.PrintNow("begin candidateTerm")
	rf.rwmu.Lock()
	if rf.role != CANDIDATE {
		rf.termAction <- OVER_TERM
		rf.rwmu.Unlock()
		return
	}

	peerNum := len(rf.peers)
	grantedVote := 1
	// 更新CANDIDATE raft信息
	rf.currentTern++
	rf.votedFor = rf.me
	// request vote
	lastLogIndex, lastTerm := rf.lastLogIndexAndTerm()
	args := &RequestVoteArgs{
		Term:         rf.currentTern,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastTerm,
	}
	voteLock := sync.Mutex{}
	cond := sync.NewCond(&voteLock)

	finished := 1
	for i := 0; i < peerNum; i++ {
		if i == rf.me {
			continue
		}
		if rf.role != CANDIDATE {
			break
		}
		go func(index int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(index, args, reply)
			if reply.Term > rf.currentTern {
				rf.changeTermTo(FOLLOWER)
				return
			}
			voteLock.Lock()
			if reply.VoteGranted {
				grantedVote++
			}
			finished++
			cond.Signal()
			voteLock.Unlock()
		}(i)
	}
	rf.rwmu.Unlock()
	//listen granted vote
	voteLock.Lock()
	for finished != peerNum && grantedVote <= peerNum/2 {
		cond.Wait()
	}
	rf.PrintNow(fmt.Sprintf("[candidateTerm]grantedVote:%d/%d", grantedVote, peerNum))
	voteLock.Unlock()

	rf.rwmu.Lock()
	if rf.role == CANDIDATE && grantedVote > peerNum/2 {
		rf.changeTermTo(LEADER)
	} else {
		rf.changeTermTo(FOLLOWER)
	}
	rf.rwmu.Unlock()
}

func (rf *Raft) leaderTerm() {
	rf.PrintNow("begin leaderTerm")
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(i int) {
			for {
				select {
				case <-rf.heartbeatTimers[i].C:
					rf.appendEntries(i)
				}
			}
		}(idx)
	}
}

func (rf *Raft) followerTerm() {
	rf.PrintNow("begin followerTerm")

}

func (rf *Raft) resetTermClick() {
	rf.PrintNow("resetTermClick")
	rf.termTimer.Reset(rf.randTermTimeout())
}
