package raft

import (
	"time"
)

func (rf *Raft) sendRegularHeartbeats() {
	// once rf ceases to be a leader, this function returns.
	for rf.identity == "leader" {
		rf.lock("HB lock")
		args := AppendEntriesArgs{
			LTerm:   rf.currentTerm,
			LID:     rf.me,
			Entries: make([]LogEntry, 0),
		}
		rf.unlock("HB lock")
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(index, &args, &reply)
				if reply.Term > args.LTerm {
					rf.lock("start_HB_change_term")
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.changeIdentity("follower")
						rf.resetElectionTimer()
						// rf.persist()
					}
					rf.unlock("start_HB_change_term")
				}
			}(index)
		}
		time.Sleep(150 * time.Millisecond) // Heartbeat interval
	}
}
