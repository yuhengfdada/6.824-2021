package raft

import (
	"time"
)

func (rf *Raft) sendRegularHeartbeats() {
	// once rf ceases to be a leader, this function returns.
	for !rf.killed() {
		rf.lock("HB lock")
		if rf.identity != "leader" {
			rf.unlock("HB lock")
			break
		}
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
						rf.receivedLargerTerm(reply.Term)
					}
					rf.unlock("start_HB_change_term")
				}
			}(index)
		}
		time.Sleep(150 * time.Millisecond) // Heartbeat interval
	}
}

func (rf *Raft) startAgreement(command interface{}) {
	rf.lock("startAgreement lock")
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	prevIndex := len(rf.log) - 2
	args := AppendEntriesArgs{
		LTerm:         rf.currentTerm,
		LID:           rf.me,
		LPrevLogIndex: prevIndex,
		LPrevLogTerm:  rf.log[prevIndex].Term,
		Entries:       rf.log[prevIndex+1:],
		LeaderCommit:  rf.commitIndex,
	}
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int) {
			for { // the leader retries AE request indefinitely.
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(index, &args, &reply)

				if reply.Term > args.LTerm {
					rf.lock("startAgreement_change_term")
					if rf.currentTerm < reply.Term {
						rf.receivedLargerTerm(reply.Term)
					}
					rf.unlock("startAgreement_change_term")
				}
				if reply.Success {
					break
				}
			}
		}(index)
	}
}
