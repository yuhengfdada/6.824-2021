package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

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

// Timer
func (rf *Raft) calcDuration() time.Duration {
	return time.Duration(rf.timeout+rand.Intn(300)) * time.Millisecond
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
		rf.resetElectionTimer()
	case "leader":
		// initialize leader-only DSs.
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			rf.nextIndex[index] = len(rf.log)
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
	DPrintf("me: %d, identity:%v,term:%d\n", rf.me, rf.identity, rf.currentTerm)
	DPrintf(format, a...)
}
