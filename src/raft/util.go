package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Misc
func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

// append
func (rf *Raft) checkConsistency(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.absoluteLength() <= args.LPrevLogIndex {
		reply.Success = false
		reply.NewNextIndex = rf.absoluteLength()
		return false

	} else if rf.findLogTermByAbsoluteIndex(args.LPrevLogIndex) != args.LPrevLogTerm {
		reply.Success = false
		reply.NewNextIndex = rf.findBadIndex(rf.findLogTermByAbsoluteIndex(args.LPrevLogIndex))
		return false
	}
	return true
}

func (rf *Raft) findBadIndex(badTerm int) int {
	if rf.lastInstalledTerm == badTerm {
		return rf.lastInstalledIndex
	}
	for index, entry := range rf.log {
		if entry.Term == badTerm {
			return rf.absoluteIndex(index)
		}
	}
	return -1
}

// snapshot
func (rf *Raft) absoluteLength() int {
	return len(rf.log) + rf.lastInstalledIndex + 1
}

func (rf *Raft) relativeIndex(absoluteIndex int) int {
	return absoluteIndex - rf.lastInstalledIndex - 1
}

func (rf *Raft) absoluteIndex(relativeIndex int) int {
	return relativeIndex + rf.lastInstalledIndex + 1
}

func (rf *Raft) findLogTermByAbsoluteIndex(absoluteIndex int) int {
	if len(rf.log) == 0 || absoluteIndex == rf.lastInstalledIndex {
		if absoluteIndex < rf.lastInstalledIndex {
			panic("findLogTermByAbsoluteIndex(): invalid index")
		}
		return rf.lastInstalledTerm
	} else {
		return rf.log[rf.relativeIndex(absoluteIndex)].Term
	}
}

// Timer
func (rf *Raft) calcDuration() time.Duration {
	return time.Duration(rf.timeout+rand.Intn(600)) * time.Millisecond
}
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.calcDuration())
}

// Role Transition
// Acquire lock before calling this function.
func (rf *Raft) changeIdentity(identity string) {
	rf.logger("change identity to " + identity)
	rf.identity = identity
	switch identity {
	case "follower":
		rf.resetElectionTimer()
	case "candidate":
		rf.currentTerm += 1
		rf.votedFor = rf.me // vote for self.
		rf.persist()
		rf.resetElectionTimer()
	case "leader":
		// initialize leader-only DSs.
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			rf.nextIndex[index] = rf.absoluteLength()
			rf.matchIndex[index] = 0
		}
		go rf.sendRegularHeartbeats()
		rf.electionTimer.Stop() // election timeout does not work on leaders.
	}
}

// received RPC that contains later termNum, convert to follower.
func (rf *Raft) receivedLargerTerm(largeTerm int) {
	rf.currentTerm = largeTerm
	rf.votedFor = -1
	rf.persist()
	rf.changeIdentity("follower")
	// rf.resetElectionTimer()
	// rf.persist()
}

// Lock wrappers
func (rf *Raft) lock(format string, a ...interface{}) {
	rf.mu.Lock()
	rf.logger("acquire lock "+format, a...)
}

func (rf *Raft) unlock(format string, a ...interface{}) {
	rf.logger("release lock "+format, a...)
	rf.mu.Unlock()
}

// Logger. Must be called with lock held.
func (rf *Raft) logger(format string, a ...interface{}) {
	DPrintf("me: %d, identity:%v, term:%d, leader:%d\n", rf.me, rf.identity, rf.currentTerm, rf.votedFor)
	DPrintf(format, a...)
}
