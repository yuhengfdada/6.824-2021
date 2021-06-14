package raft

// on election timeout, start election.
func (rf *Raft) startElection() {
	rf.lock("start-election")
	rf.changeIdentity("Candidate")
	// fill in args.
	args := RequestVoteArgs{}
	args.CID = rf.me
	args.CLastLogIndex = len(rf.log) - 1
	args.CLastLogTerm = rf.log[args.CLastLogIndex].Term
	args.CTerm = rf.currentTerm
	rf.unlock("start-election")
	// send and receive.
	// PROBLEM: what if the election timer goes off while the current thread is waiting?
	// how to synchronize?

	// Note that self-vote is counted on init.
	grantedCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.CTerm {
				rf.lock("start_ele_change_term")
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeIdentity("follower")
					rf.resetElectionTimer()
					// rf.persist()
				}
				rf.unlock("start_ele_change_term")
			}
		}(votesCh, index)
	}

	for {
		r := <-votesCh
		chResCount += 1
		if r == true {
			grantedCount += 1
		}
		// all votes received || won majority || lose majority
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		rf.logger("grantedCount <= len/2:count:%d", grantedCount)
		return
	}

	rf.lock("start_ele2")
	rf.logger("before try change to leader,count:%d, args:%+v", grantedCount, args)
	if rf.currentTerm == args.CTerm && rf.identity == "candidate" {
		rf.changeIdentity("leader")
		rf.persist()
	}
	/*
		if rf.identity == "leader" {
			rf.resetHeartBeatTimers()
		}
	*/
	rf.unlock("start_ele2")
}
