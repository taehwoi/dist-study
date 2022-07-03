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
	//	"bytes"

	"context"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"golang.org/x/sync/errgroup"
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

const (
	ElectionTimeOutMin = 500
	ElectionTimeOutMax = ElectionTimeOutMin + 300
)

const (
	Leader int32 = iota
	Candidate
	Follower
)

type event int

const (
	START event = iota
	ELECTION_TIMEOUT
	RECIEVED_MAJOR_VOTES
	SERVER_WITH_HIGHER_TERM
	CURRENT_LEADER_FOUND
)

type Log struct {
	Term int
}

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
	status    int32               // Leader, Follower, Candidate

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor    int
	currentTerm int
	logs        []*Log

	// notifies that a heartbeat from the leader has arrived
	heartbeatCh   chan struct{}
	voteRequestCh chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	log.Println("GetState called")

	var term int
	var isleader bool

	log.Printf("%d trying to acquire lock in GetState...\n", rf.me)
	rf.mu.Lock()
	log.Println("acquired lock in GetState...")
	term = rf.currentTerm
	rf.mu.Unlock()
	isleader = rf.currentStatus() == Leader
	log.Println("GetState called finished")

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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("%d received a vote request from %d\n", rf.me, args.CandidateId)

	// notify that a vote request has arrived
	rf.voteRequestCh <- struct{}{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("%d not voting for %d\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// update term
		rf.currentTerm = args.Term
		rf.updateStatus(Follower)
		// since we updated term, we reset votedFor; we haven't voted for anyone in this term (yet)
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		log.Printf("%d can vote for %d, check logs", rf.me, args.CandidateId)
		lastLogIndex := len(rf.logs) - 1
		if args.LastLogTerm >= rf.logs[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex {
			log.Printf("%d voted for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
		} else {
			log.Printf("%d did not vote for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.votedFor = -1
		}

	}
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
func (rf *Raft) sendRequestVote(ctx context.Context,
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply) bool {

	log.Printf("%d send request vote to %d\n", rf.me, server)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// TODO: should this code check if the appendEntry request is from a valid leader?
	// received heartbeat
	rf.heartbeatCh <- struct{}{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// the appendEntry came from an legitimate leader
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateStatus(Follower)
		rf.votedFor = -1
	}

	// 2. if log doesn't contain an entry at prevLogIndex...
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// 3. TODO
	// 4. TODO
	// 5. TODO

	if rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {

	}

}

func (rf *Raft) sendAppendEntries(ctx context.Context,
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {

	log.Printf("%d send append entry to %d\n", rf.me, server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// TODO: use context to cancel all running goroutines
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		r := ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
		t := time.Duration(r) * time.Millisecond
		// r can be infinite if leader
		if rf.currentStatus() == Leader {
			// 300 years
			t = time.Duration(math.MaxInt64)
		}

		select {
		case <-rf.heartbeatCh:
			log.Printf("%d received heartbeat before election timeout\n", rf.me)
		case <-rf.voteRequestCh:
			log.Printf("%d received voterequest before election timeout\n", rf.me)
		case <-time.After(t):
			// start election
			// i am a new candidate, or was a candidate
			rf.updateStatus(Candidate)
			log.Printf("%d: start election after %d(ms)\n", rf.me, r)
			// TODO: cancel election if receiving heartbeat
			go rf.startElection(context.TODO())
		}
	}
}

func (rf *Raft) startElection(ctx context.Context) {
	// TODO: cancel election if receiving heartbeat
	log.Printf("%d: starting election\n", rf.me)
	rf.mu.Lock()
	// increment current term
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me

	lastLogIndex := len(rf.logs) - 1
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.logs[lastLogIndex].Term, // TODO
	}
	rf.mu.Unlock()

	replyCh := rf.requestVoteToPeers(ctx, &req)

	log.Printf("%d counting votes\n", rf.me)

	votes := 1 // includes my vote
	// for each incoming vote replies
	for reply := range replyCh {
		rf.mu.Lock()

		if rf.currentTerm != req.Term {
			log.Printf("%d received an old reply\n", rf.me)
			rf.mu.Unlock()
			return
		}

		// update term
		rf.currentTerm = Max(reply.Term, rf.currentTerm)
		rf.mu.Unlock()

		if reply.VoteGranted {
			votes++
		}
		// if we have recieved majority of votes, we don't have to wait for rest of replies
		// we leave the replyCh open, so that the broadcasting go routine can close it
		if votes > len(rf.peers)/2 {
			log.Printf("%d recieved %d / %d votes\n", rf.me, votes, len(rf.peers))
			break
		}
	}

	log.Printf("%d trying to become a leader...\n", rf.me)
	succ := rf.tryUpdateStatus(Candidate, Leader)
	if !succ {
		// TODO: give up earlier
		log.Printf("%d is not a candidate anymore; give up election", rf.me)
		return
	}
	log.Printf("%d became a leader!\n", rf.me)

	// send empty appendEntry as a heartbeat to peers, as a new leader
	go rf.sendHeartBeat(ctx)
}

func (rf *Raft) sendHeartBeat(ctx context.Context) {

	for rf.currentStatus() == Leader {

		rf.mu.Lock()
		req := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.logs) - 1,
			PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
			Entries:      []*Log{},
			LeaderCommit: -1,
		}
		rf.mu.Unlock()

		go rf.sendAppendEntriesToPeers(ctx, req)

		time.Sleep(200 * time.Millisecond)
	}

}

func (rf *Raft) sendAppendEntriesToPeers(ctx context.Context, req *AppendEntriesArgs) chan *AppendEntriesReply {
	replies := make([]AppendEntriesReply, len(rf.peers))
	replyCh := make(chan *AppendEntriesReply, len(rf.peers)-1)

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(len(rf.peers) - 1)
	for idx := range rf.peers {
		server := idx
		if server != rf.me {
			eg.Go(func() error {
				if ok := rf.sendAppendEntries(ctx, server, req, &replies[server]); !ok {
					replies[server] = AppendEntriesReply{}
					log.Println("failed rpc request for append entry")
				}
				replyCh <- &replies[server]
				return nil
			})
		}
	}
	// NOTE: we can wait on all Calls because Call() is guaranteed to return.

	go func() {
		defer close(replyCh)
		eg.Wait()
		log.Println("got all replies from rpc request for append entry")
	}()

	return replyCh
}

func (rf *Raft) requestVoteToPeers(ctx context.Context, req *RequestVoteArgs) chan *RequestVoteReply {

	// we actually need rf.peers - 1, but this makes indexing easier
	replies := make([]RequestVoteReply, len(rf.peers))
	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(len(rf.peers) - 1)
	for idx := range rf.peers {
		server := idx
		if server != rf.me {
			eg.Go(func() error {
				if ok := rf.sendRequestVote(ctx, server, req, &replies[server]); !ok {
					replies[server] = RequestVoteReply{VoteGranted: false}
					log.Printf("%d failed rpc request for request vote\n", rf.me)
				}
				replyCh <- &replies[server]
				return nil
			})
		}
	}

	go func() {
		defer close(replyCh)
		eg.Wait()
		log.Println("got all replies from rpc request for request vote")
	}()

	return replyCh
}

func (rf *Raft) currentStatus() int32 {
	return atomic.LoadInt32(&rf.status)
}

func (rf *Raft) updateStatus(status int32) {
	if status == Leader {
		log.Printf("%d updating its status to leader\n", rf.me)
	}
	if status == Candidate {
		log.Printf("%d updating its status to candidate\n", rf.me)
	}
	if status == Follower {
		log.Printf("%d updating its status to follower\n", rf.me)
	}
	atomic.StoreInt32(&rf.status, status)
}

func (rf *Raft) tryUpdateStatus(old int32, new int32) bool {
	return atomic.CompareAndSwapInt32(&rf.status, old, new)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	// servers start as followers
	rf.status = Follower

	// a dummy log to make getting lastLogIndex and Term easier
	sentinelLog := Log{Term: 0}
	rf.logs = []*Log{&sentinelLog}
	rf.heartbeatCh = make(chan struct{})
	rf.voteRequestCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
