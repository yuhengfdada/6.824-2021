package raft

import (
	"time"
)

func (rf *Raft) sendRegularHeartbeats() {
	// once rf ceases to be a leader, this function returns.
	for !rf.killed() {
		rf.lock("HB lock")
		if rf.identity != "leader" {
			rf.unlock("HB not leader lock")
			break
		}
		args := AppendEntriesArgs{
			LTerm:        rf.currentTerm,
			LID:          rf.me,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
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

// A new command comes from client. Start synchronizing with replicas.
// Only supports ONE command.
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
	rf.unlock("startAgreement lock")
	successCount := 1
	chResCount := 1
	rChan := make(chan bool, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(rChan chan bool, index int) {
			//for { // the leader retries AE request indefinitely.
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(index, &args, &reply)
			rChan <- reply.Success
			if reply.Term > args.LTerm {
				rf.lock("startAgreement_change_term")
				if rf.currentTerm < reply.Term {
					rf.receivedLargerTerm(reply.Term)
				}
				rf.unlock("startAgreement_change_term")
			}
			//}
		}(rChan, index)
	}
	for !rf.killed() {
		// 其实有可能一直卡在这...
		r := <-rChan
		chResCount += 1
		if r {
			successCount += 1
		}
		// all replies received || majority has replicated || majority failed to replicate
		if chResCount == len(rf.peers) || successCount > len(rf.peers)/2 || chResCount-successCount > len(rf.peers)/2 {
			break
		}
	}
	if successCount <= len(rf.peers)/2 {
		rf.lock("replicate fail lock")
		defer rf.unlock("replicate fail lock")
		rf.logger("successCount <= len/2:count:%d", successCount)
		return
	}
	// Now, the command is replicated on a majority of machines. We can start committing + applying.
	rf.lock("apply command lock")
	rf.commitIndex = args.LPrevLogIndex + 1
	rf.sendApplyMsg(rf.commitIndex)
	rf.unlock("apply command lock")
}

func (rf *Raft) sendApplyMsg(index int) {
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.log[index].Command,
		CommandIndex: index,
	}
	rf.applyCh <- applyMsg
}
