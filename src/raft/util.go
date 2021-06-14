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

// Timer
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(rf.timeout+rand.Intn(300)) * time.Millisecond)
}

// Role Transition
// Acquire lock before calling this function.
func (rf *Raft) changeIdentity(identity string) {
	rf.identity = identity
	switch identity {
	case "follower":
		rf.resetElectionTimer()
	case "candidate":
		rf.currentTerm += 1
		rf.votedFor = rf.me // vote for self.
		rf.resetElectionTimer()
	case "leader":
		go rf.sendRegularHeartbeats()
		rf.electionTimer.Stop() // election timeout does not work on leaders.
	}
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

// Logger
func (rf *Raft) logger(format string, a ...interface{}) {
	DPrintf("me: %d, identity:%v,term:%d\n", rf.me, rf.identity, rf.currentTerm)
	DPrintf(format, a...)
}
