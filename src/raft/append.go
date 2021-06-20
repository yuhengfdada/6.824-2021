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
		rf.nextIndex[index] -= 1
		rf.unlock("force update failed lock")
	}
}

// A new command comes from client. Start synchronizing with replicas.
// Only supports ONE command.
func (rf *Raft) startAgreement(command interface{}, logLength int) {
	rf.lock("startAgreement lock")

	rf.logger("the last log index is currently %d", logLength-1)
	// prevIndex := logLength - 2
	/*
		args := AppendEntriesArgs{
			LTerm:         rf.currentTerm,
			LID:           rf.me,
			LPrevLogIndex: prevIndex,
			LPrevLogTerm:  rf.log[prevIndex].Term,
			Entries:       rf.log[prevIndex+1:],
			LeaderCommit:  rf.commitIndex,
		}
	*/
	rf.unlock("startAgreement lock")
	successCount := 1
	rChan := make(chan bool, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(rChan chan bool, index int) {
			for !rf.killed() { // the leader retries AE request indefinitely.
				rf.lock("agreement lock 2")
				if rf.identity != "leader" {
					rf.unlock("agreement lock 2_not leader")
					return
				}
				nextIndex := rf.nextIndex[index]
				staleFollower := logLength > nextIndex
				args := AppendEntriesArgs{}
				if staleFollower {
					args.LTerm = rf.currentTerm
					args.LID = rf.me
					args.Entries = rf.log[nextIndex:]
					args.LPrevLogIndex = nextIndex - 1
					args.LPrevLogTerm = rf.log[nextIndex-1].Term
					args.LeaderCommit = rf.commitIndex
				} else {
					rf.unlock("agreement lock 2")
					return
				}
				rf.unlock("agreement lock 2")
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, &args, &reply)
				if !ok {
					time.Sleep(300 * time.Millisecond)
					continue
				}
				if reply.Term > args.LTerm {
					rf.lock("startAgreement_change_term")
					if rf.currentTerm < reply.Term {
						// rf.log = rf.log[:len(rf.log)-1] // rewind the append.
						// logLength -= 1
						// rf.printLog() //debug
						rf.receivedLargerTerm(reply.Term)
						rf.unlock("startAgreement_change_term")
						return
					}
					rf.unlock("startAgreement_change_term")
				}
				if reply.Success {
					rf.lock("agreement success lock")
					rf.nextIndex[index] = logLength
					rf.matchIndex[index] = logLength - 1
					// figure 2 last sentence. Not easy to implement!
					rf.checkMatchIndex()
					rf.unlock("agreement success lock")
					rChan <- reply.Success
					break
				}
				rf.lock("agreement failed lock")
				rf.nextIndex[index] -= 1
				rf.unlock("agreement failed lock")
				time.Sleep(300 * time.Millisecond)
			}
		}(rChan, index)
	}
	for !rf.killed() && rf.identity == "leader" {
		r := <-rChan
		// chResCount += 1
		if r {
			successCount += 1
		}
		// all replies received || majority has replicated || majority failed to replicate
		if successCount > len(rf.peers)/2 {
			break
		}
	}
	/*
		if successCount <= len(rf.peers)/2 {
			rf.lock("replicate fail lock")
			defer rf.unlock("replicate fail lock")
			rf.logger("successCount <= len/2:count:%d", successCount)
			return
		}
	*/
	// Now, the command is replicated on a majority of machines. We can start committing + applying.
	rf.lock("apply command lock")
	if rf.identity == "leader" {
		// concurrent Start()s: wait for previous commands to be applied
		for rf.lastApplied < logLength-2 {
			if rf.identity != "leader" {
				rf.unlock("apply command lock")
				return
			}
			rf.logger("cond_wait! Last applied index: %d Wanna commit index: %d", rf.lastApplied, logLength-1)
			rf.cond.Wait()
		}

		rf.commitIndex = logLength - 1
		rf.logger("going to commit index %d", rf.commitIndex)
		rf.sendApplyMsg(rf.commitIndex)
		rf.lastApplied = rf.commitIndex
	}
	rf.unlock("apply command lock")
	rf.cond.Broadcast()
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
