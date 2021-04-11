package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	AppendHeartbeat = 200
	ElecTimeoutMin  = 700
	ElecTimeoutMax  = 1900
)

type Status int

const (
	Follower Status = 1 + iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	currentLeader int
	votedTerm     int
	votedFor      int
	log           []int

	// volatile
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	status Status
	HBCancel      context.CancelFunc
	electionReset chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == Leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionReset:
			continue
		case <-time.After(time.Duration(ElecTimeoutMin+rand.Intn(ElecTimeoutMax-ElecTimeoutMin)) * time.Millisecond):
			go rf.election()
		}
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	fmt.Printf("%d init election for term %d\n", rf.me, rf.currentTerm+1)
	rf.status = Candidate
	rf.currentTerm += 1
	rf.votedTerm += 1
	rf.votedFor = rf.me
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CadidateId:   rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rf.mu.Unlock()
	rf.electionReset <- struct{}{}

	var votes int32 = 1
	var response int32 = 1
	voteDone := make(chan struct{})
	responseDone := make(chan struct{})
	closed := sync.Once{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peerId int) {
			resp := new(RequestVoteReply)
			ok := rf.sendRequestVote(peerId, req, resp)
			if !ok {
				//fmt.Printf("[LOSS][%d] NO RESPOND from %d\n", rf.me, peerId)
				return
			}

			if resp.VoteGranted {
				atomic.AddInt32(&votes, 1)
				votes := atomic.LoadInt32(&votes)
				if int(votes) > len(rf.peers)/2 {
					closed.Do(func() {
						close(voteDone)
					})
				}
				//fmt.Printf("[APPROVE][%d] approved by %d\n", rf.me, peerId)
			} else {
				//fmt.Printf("[REJ][%d] rejected by %d\n", rf.me, peerId)
			}
			atomic.AddInt32(&response, 1)
			response := atomic.LoadInt32(&response)
			if int(response) == len(rf.peers) {
				close(responseDone)
			}
		}(i)
	}

	// waits for results
	select {
	case <-voteDone:
		// become leader
		fmt.Printf("%d becomes leader for term %d\n\n", rf.me, rf.currentTerm)
		rf.mu.Lock()
		rf.status = Leader
		rf.currentLeader = rf.me

		// send heartbeat
		ctx, cancel := context.WithCancel(context.Background())
		rf.HBCancel = cancel
		rf.mu.Unlock()
		rf.leaderHB(ctx)
	case <-responseDone:
		// rejected
		fmt.Printf("REJECTED\n")
	case <-time.After(time.Duration(ElecTimeoutMin) * time.Millisecond):
		// failed election
		fmt.Printf("ELEC TIMEOUT[%d]\n", rf.me)
	}
}

// leaderHB sends heartbeats to followers
func (rf *Raft) leaderHB(ctx context.Context) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		req2 := &AppendArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			go func(peerId int) {
				resp := new(AppendReply)
				_ = rf.sendAppend(peerId, req2, resp)
			}(i)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(AppendHeartbeat) * time.Millisecond):
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).
	rf.electionReset = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
