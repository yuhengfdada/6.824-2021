package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CTerm         int
	CID           int
	CLastLogIndex int
	CLastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// helper function to determine whether to vote in RVHandler.
// Acquire lock before calling this function.
func (rf *Raft) isCandidateMoreUTD(args *RequestVoteArgs) bool {
	lastIndex := len(rf.log) - 1
	if args.CLastLogTerm > rf.log[lastIndex].Term {
		return true
	}
	if args.CLastLogTerm == rf.log[lastIndex].Term {
		if args.CLastLogIndex >= lastIndex {
			return true
		}
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("RVHandler lock")
	reply.Term = rf.currentTerm // update requester to follower (if currentTerm > args.CTerm)

	if args.CTerm < rf.currentTerm {
		reply.VoteGranted = false
		rf.unlock("RVHandler lock")
		return
	}

	if args.CTerm > rf.currentTerm {
		rf.currentTerm = args.CTerm
		rf.identity = "follower"
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CID) && rf.isCandidateMoreUTD(args) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.unlock("RVHandler lock")
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	LTerm         int
	LID           int
	LPrevLogIndex int
	LPrevLogTerm  int
	Entries       []LogEntry
	LeaderCommit  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}
