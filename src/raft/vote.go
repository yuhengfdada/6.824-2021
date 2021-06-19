package raft

// on election timeout, start election.
func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	rf.lock("start-election")
	rf.changeIdentity("candidate")
	// fill in args.
	args := RequestVoteArgs{}
	args.CID = rf.me
	args.CLastLogIndex = len(rf.log) - 1
	args.CLastLogTerm = rf.log[args.CLastLogIndex].Term
	args.CTerm = rf.currentTerm
	// rf.logger("args.currentTerm: "itoa(args.CTerm))
	rf.unlock("start-election")
	// send and receive.
	// PROBLEM: what if the election timer goes off while the current thread is waiting?
	// how to synchronize?

	// Note that self-vote is counted on init.
	grantedCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			// rf.logger("got reply from index "+Itoa(index)+" , voteGranted = "+)
			ch <- reply.VoteGranted
			if reply.Term > args.CTerm {
				rf.lock("start_ele_change_term")
				if rf.currentTerm < reply.Term {
					rf.receivedLargerTerm(reply.Term)
				}
				rf.unlock("start_ele_change_term")
			}
		}(votesCh, index)
	}

	for !rf.killed() {
		// 其实有可能一直卡在这...
		r := <-votesCh
		chResCount += 1
		if r {
			grantedCount += 1
		}
		// all votes received || won majority || lose majority
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		rf.lock("elec fail lock")
		defer rf.unlock("elec fail lock")
		rf.logger("grantedCount <= len/2:count:%d", grantedCount)
		return
	}

	rf.lock("start_ele2")
	rf.logger("before try change to leader,count:%d, args:%+v", grantedCount, args)
	if rf.currentTerm == args.CTerm && rf.identity == "candidate" {
		rf.changeIdentity("leader")
		// rf.persist()
	}
	/*
		if rf.identity == "leader" {
			rf.resetHeartBeatTimers()
		}
	*/
	rf.unlock("start_ele2")
}
