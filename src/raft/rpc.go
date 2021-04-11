package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CadidateId   int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

// Append RPC
type AppendArgs struct {
	Term        int
	LeaderId    int
	PrevLogIdx  int
	PrevLogTerm int
	Entries     []int

	LeaderCommit int
}

type AppendReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.votedTerm || (args.Term == rf.votedTerm && args.CadidateId == rf.votedFor) {
		rf.electionReset <- struct{}{}
		rf.votedTerm = args.Term
		rf.votedFor = args.CadidateId
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
	reply.CurrentTerm = rf.currentTerm

}

func (rf *Raft) AppendHandler(args *AppendArgs, reply *AppendReply) {
	//fmt.Printf("[HB][%d] from %d term=%d\n", rf.me, args.LeaderId, args.Term)

	rf.mu.Lock()
	// new term || new leader elected || current leader
	if rf.currentTerm < args.Term || (rf.status == Candidate && rf.currentTerm <= args.Term) ||
		(rf.currentTerm == args.Term && rf.currentLeader == args.LeaderId) {
		rf.electionReset <- struct{}{}
		rf.currentTerm = args.Term
		rf.currentLeader = args.LeaderId

		if rf.me != args.LeaderId {
			rf.status = Follower
			if rf.HBCancel != nil {
				rf.HBCancel()
			}
		}
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppend(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendHandler", args, reply)
	return ok
}
