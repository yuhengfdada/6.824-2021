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
			LTerm:         rf.currentTerm,
			LID:           rf.me,
			LPrevLogIndex: len(rf.log) - 1,
			LPrevLogTerm:  rf.log[len(rf.log)-1].Term,
			Entries:       make([]LogEntry, 0),
			LeaderCommit:  rf.commitIndex,
		}
		rf.unlock("HB lock")
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, &args, &reply)
				if !ok {
					return
				}
				if reply.Term > args.LTerm {
					rf.lock("start_HB_change_term")
					if rf.currentTerm < reply.Term {
						rf.receivedLargerTerm(reply.Term)
					}
					rf.unlock("start_HB_change_term")
				} else if !reply.Success { // if log inconsistency detected, force update
					rf.forceUpdate(index)
				}

			}(index)
		}
		time.Sleep(150 * time.Millisecond) // Heartbeat interval
	}
}

// precondition: inconsistent log
func (rf *Raft) forceUpdate(index int) {
	for !rf.killed() {
		rf.lock("force update lock")
		if rf.identity != "leader" {
			rf.unlock("force update lock_not leader")
			return
		}
		logLength := len(rf.log)
		nextIndex := rf.nextIndex[index]
		args := AppendEntriesArgs{}
		args.LTerm = rf.currentTerm
		args.LID = rf.me
		args.Entries = rf.log[nextIndex:]
		args.LPrevLogIndex = nextIndex - 1
		args.LPrevLogTerm = rf.log[nextIndex-1].Term
		args.LeaderCommit = rf.commitIndex

		rf.unlock("force update lock")
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(index, &args, &reply)
		if !ok {
			return
		}
		if reply.Term > args.LTerm {
			rf.lock("force update_change_term")
			if rf.currentTerm < reply.Term {
				rf.receivedLargerTerm(reply.Term)
				rf.unlock("force update_change_term")
				return
			}
			rf.unlock("force update_change_term")
		}
		if reply.Success {
			rf.lock("force update success lock")
			rf.nextIndex[index] = logLength
			rf.matchIndex[index] = logLength - 1
			// figure 2 last sentence. Not easy to implement!
			rf.checkMatchIndex()
			rf.unlock("force update success lock")
			return
		}
		rf.lock("force update failed lock")
		rf.logger("reply from server %d, with NewNextIndex = %d", index, reply.NewNextIndex)
		rf.nextIndex[index] = reply.NewNextIndex
		rf.unlock("force update failed lock")
	}
}

// Hold lock before calling this function.
func (rf *Raft) checkMatchIndex() {
	//自己永远match到最后
	rf.matchIndex[rf.me] = len(rf.log) - 1
	// check condition.
	threshold := len(rf.peers) / 2
	count := 0
	minValue := 0
	for _, value := range rf.matchIndex {
		if value > rf.commitIndex {
			count += 1
			if value < minValue || minValue == 0 {
				minValue = value
			}
		}
	}
	if count > threshold {
		for i := minValue; i > rf.commitIndex; i-- {
			if rf.log[i].Term == rf.currentTerm {
				prevCIndex := rf.commitIndex
				rf.commitIndex = i
				// don't forget to check whether commitIndex > lastApplied. If so, apply new commits.
				for j := prevCIndex + 1; j <= rf.commitIndex; j++ {
					rf.sendApplyMsg(j)
				}
				rf.lastApplied = rf.commitIndex
				break
			}
		}
	}

}

func (rf *Raft) sendApplyMsg(index int) {
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.log[index].Command,
		CommandIndex: index,
	}
	rf.applyCh <- applyMsg
}
