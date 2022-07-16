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
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"sync"
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

type status int32

const (
	UNDEFINED status = iota
	LEADER
	CANDIDATE
	FOLLOWER
)

func (s status) String() string {
	switch s {
	case UNDEFINED:
		return "UNDEFINED"
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	case FOLLOWER:
		return "FOLLOWER"
	}
	return "UNKNOWN"
}

type event int

const (
	STARTED event = iota
	ELECTION_TIMEOUTED
	MAJOR_VOTES_RECEIVED
	HIGHER_TERM_FOUND
	CURRENT_LEADER_FOUND
	COMMIT_INDEX_GT_LAST_APPLIED
)

func (e event) String() string {
	switch e {
	case STARTED:
		return "STARTED"
	case ELECTION_TIMEOUTED:
		return "ELECTION_TIMEOUTED"
	case MAJOR_VOTES_RECEIVED:
		return "MAJOR_VOTES_RECEIVED"
	case HIGHER_TERM_FOUND:
		return "HIGHER_TERM_FOUND"
	case CURRENT_LEADER_FOUND:
		return "CURRENT_LEADER_FOUND"
	case COMMIT_INDEX_GT_LAST_APPLIED:
		return "COMMIT_INDEX_GT_LAST_APPLIED"
	}
	return "UNKNOWN"
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

func (l Log) String() string {

	return fmt.Sprintf(
		"Log{T: %d, I: %d, C: %v}",
		l.Term, l.Index, l.Command)
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
	status    status              // Leader, Follower, Candidate

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor    int
	currentTerm int
	logs        []*Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// notifies that a heartbeat from the leader has arrived
	heartbeatCh   chan struct{}
	voteRequestCh chan struct{}

	applyCh chan ApplyMsg

	lastNewEntryIndex int

	mainCancelFunc   func()
	leaderCancelFunc func()
	leaderContext    context.Context

	followerContext    context.Context
	followerCancelFunc func()
}

func (rf *Raft) handleEvent(ctx context.Context, e event, data int) status {
	log.Printf("%d Handling event: %s at term %d\n", rf.me, e, rf.currentTerm)

	switch rf.status {
	case UNDEFINED:
		if e == STARTED {
			rf.status = FOLLOWER
			log.Printf("%d: UNDEFINED => STARTED\n", rf.me)
			// start ticker goroutine to start elections
			go rf.ticker(ctx)
		}
	case FOLLOWER:
		if e == ELECTION_TIMEOUTED {
			log.Printf("%d: FOLLOWER => CANDIDATE\n", rf.me)
			rf.status = CANDIDATE
			rf.followerContext, rf.followerCancelFunc = context.WithCancel(ctx)
			go rf.startElection(context.TODO())
		} else if e == HIGHER_TERM_FOUND {
			log.Printf("%d: FOLLOWER => FOLLOWER\n", rf.me)
			rf.handleHigherTermFound(data)
		}
	case CANDIDATE:
		if e == MAJOR_VOTES_RECEIVED {
			log.Printf("%d: CANDIDATE => LEADER\n", rf.me)
			rf.status = LEADER
			rf.followerCancelFunc()
			rf.followerContext = nil

			rf.nextIndex = make([]int, len(rf.peers))
			for idx := range rf.nextIndex {
				rf.nextIndex[idx] = len(rf.logs) + 1
			}

			rf.matchIndex = make([]int, len(rf.peers))
			for idx := range rf.matchIndex {
				rf.matchIndex[idx] = 0
			}
			rf.leaderContext, rf.leaderCancelFunc = context.WithCancel(ctx)

			// send heart beat as new leader
			go rf.sendHeartBeat(rf.leaderContext)
			go rf.tryApply(rf.leaderContext)

			// start begin entries for once
			// for server := range rf.peers {
			// 	if server != rf.me {
			// 		go rf.beginEntriesAgreement(server)
			// 	}
			// }
		} else if e == CURRENT_LEADER_FOUND {
			log.Printf("%d: CANDIDATE => FOLLOWER\n", rf.me)
			rf.followerCancelFunc()
			rf.followerContext = nil
			rf.status = FOLLOWER
		} else if e == HIGHER_TERM_FOUND {
			log.Printf("%d: CANDIDATE => FOLLOWER\n", rf.me)
			rf.followerCancelFunc()
			rf.followerContext = nil
			rf.handleHigherTermFound(data)
		} else if e == ELECTION_TIMEOUTED {
			log.Printf("%d: CANDIDATE => CANDIDATE\n", rf.me)
			go rf.startElection(ctx)
		}
	case LEADER:
		if e == HIGHER_TERM_FOUND {
			log.Printf("%d: LEADER => FOLLOWER\n", rf.me)
			rf.leaderCancelFunc()
			rf.leaderContext = nil
			rf.handleHigherTermFound(data)
		} else if e == CURRENT_LEADER_FOUND {
			log.Printf("%d: LEADER => FOLLOWER\n", rf.me)
			rf.leaderCancelFunc()
			rf.leaderContext = nil
			rf.status = FOLLOWER
		}
	}

	return rf.status
}

func (rf *Raft) handleHigherTermFound(term int) {
	// update term

	rf.status = FOLLOWER
	rf.currentTerm = term
	// since we updated term, we reset votedFor; we haven't voted for anyone in this new term (yet)
	rf.votedFor = -1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	log.Println("GetState called")

	var term int
	var isleader bool

	log.Printf("%d trying to acquire lock in GetState...\n", rf.me)
	rf.mu.Lock()
	log.Printf("%d acquired lock in GetState...", rf.me)
	term = rf.currentTerm
	isleader = (rf.status == LEADER)
	rf.mu.Unlock()
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

func (a *RequestVoteArgs) String() string {
	return fmt.Sprintf(
		"RequestVoteArgs{T: %d, CID: %d, LLI: %d, LLT %d}",
		a.Term, a.CandidateId, a.LastLogIndex, a.LastLogTerm)
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

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"AppendEntriesArgs{T: %d, L: %d, PLI: %d, PLT %d, E: %v, LC: %d}",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.Entries, a.LeaderCommit)
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
	// rf.voteRequestCh <- struct{}{}

	//write lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("%d not voting for %d\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.handleEvent(context.TODO(), HIGHER_TERM_FOUND, args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		log.Printf("%d can vote for %d, check logs", rf.me, args.CandidateId)
		log.Printf("%d can vote for, args: %v", rf.me, args)
		lastLogIndex := len(rf.logs)
		if lastLogIndex == 0 && args.LastLogTerm == -1 {
			log.Printf("%d voted for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			go func() {
				rf.voteRequestCh <- struct{}{}
			}()
			return
		}

		if args.LastLogTerm > rf.logs[lastLogIndex-1].Term {
			log.Printf("%d voted for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			go func() {
				rf.voteRequestCh <- struct{}{}
			}()
		} else if args.LastLogTerm == rf.logs[lastLogIndex-1].Term && args.LastLogIndex >= lastLogIndex {
			log.Printf("%d voted for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			go func() {
				rf.voteRequestCh <- struct{}{}
			}()
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

	// TODO: should this code check if the appendEntry request is from a valid leader? <- YES
	// received heartbeat

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		log.Printf("%d received valid heartbeat\n", rf.me)
		// received heartbeat
		rf.handleEvent(context.TODO(), CURRENT_LEADER_FOUND, 0)
		go func() {
			rf.heartbeatCh <- struct{}{}
		}()
	}

	if args.Term > rf.currentTerm {
		rf.handleEvent(context.TODO(), HIGHER_TERM_FOUND, args.Term)
	}

	log.Printf("%d, args: %v", rf.me, args)
	log.Printf("%d's current logs: %v, pli: %d", rf.me, rf.logs, args.PrevLogIndex)

	reply.Term = rf.currentTerm

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("reply false because %d has higher term %d than %d", rf.me, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.logs) {
		reply.Success = false
		return
	}
	// 2. if log doesn't contain an entry at prevLogIndex...
	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true

	//FIXME!
	for _, val := range args.Entries {
		if val.Index > len(rf.logs) {
			break //FIXME?
		}
		// same index, but diffrent terms
		if rf.logs[val.Index-1].Term != val.Term {
			// delete the existing entry and all that follow it
			// by only retaining the entries before it
			rf.logs = rf.logs[:val.Index-1]
			break
		}
	}

	for _, val := range args.Entries {
		if val.Index > len(rf.logs) {
			log.Printf("%d appending %v to %v", rf.me, val, rf.logs)
			rf.logs = append(rf.logs, val)
			rf.lastNewEntryIndex = val.Index
			log.Printf("%d lastNewEntryINdex %d", rf.me, rf.lastNewEntryIndex)
		}
	}

	log.Printf("%d added args.Entries %v to its logs %v", rf.me, args.Entries, rf.logs)

	if args.LeaderCommit > rf.commitIndex {
		log.Printf("%d leader commit updated %d, %d, %d", rf.me, args.LeaderCommit, rf.commitIndex, rf.lastNewEntryIndex)
		prev := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, rf.lastNewEntryIndex)
		log.Printf("%d new commit index %d", rf.me, rf.commitIndex)
		if prev != rf.commitIndex {
			go rf.tryCommit()
		}
	}

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
	rf.mu.Lock()

	index := len(rf.logs) + 1
	term := rf.currentTerm
	isLeader := (rf.status == LEADER)

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	log.Printf("%d received command %v", rf.me, command)
	entry := &Log{
		Term:    term,
		Index:   index,
		Command: command,
	}

	rf.logs = append(rf.logs, entry)

	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me {

			go rf.beginEntriesAgreement(rf.leaderContext, server)
		}
	}

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) beginEntriesAgreement(ctx context.Context, server int) {

	select {
	case <-ctx.Done():
		return
	default:
	}

	// read lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d, beginEntriesAgreement NextIndex: %v", rf.me, rf.nextIndex)

	log.Printf("%d=>%d NextIndex: %v", rf.me, server, rf.nextIndex[server])
	log.Printf("%d=>%d MatchIndex: %v", rf.me, server, rf.matchIndex[server])
	log.Printf("%d Logs: %v", rf.me, rf.logs)

	lastLogIndex := len(rf.logs)

	if lastLogIndex >= rf.nextIndex[server] {
		entries := make([]*Log, 0)

		next := rf.nextIndex[server]
		entries = append(entries, rf.logs[next-1:]...)

		var prevLogIndex int
		var prevLogTerm int

		if next == 1 {
			prevLogIndex = 0
			prevLogTerm = -1
		} else {
			prevLogIndex = rf.logs[next-2].Index
			prevLogTerm = rf.logs[next-2].Term
		}
		req := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      entries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntriesToPeer(ctx, req, server)
	}
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
	rf.mainCancelFunc()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(ctx context.Context) {
	for {
		r := ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
		t := time.Duration(r) * time.Millisecond
		// r can be infinite if leader
		rf.mu.Lock()
		if rf.status == LEADER {
			// 300 years
			t = time.Duration(math.MaxInt64)
			log.Printf("setting a large timer %d", t)
		}
		rf.mu.Unlock()

		select {
		case <-rf.heartbeatCh:
			log.Printf("%d received heartbeat before election timeout\n", rf.me)
		case <-rf.voteRequestCh:
			log.Printf("%d received voterequest before election timeout\n", rf.me)
		case <-time.After(t):
			// TODO: cancel election if receiving heartbeat
			rf.mu.Lock()
			rf.handleEvent(ctx, ELECTION_TIMEOUTED, 0)
			rf.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) sendHeartBeat(ctx context.Context) {

	for {
		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			rf.mu.Lock()
			next := rf.nextIndex[server]

			var prevLogIndex int
			var prevLogTerm int

			log.Printf("%d, next: %d", rf.me, next)
			if next == 1 {
				prevLogIndex = 0
				prevLogTerm = -1
			} else {
				prevLogIndex = rf.logs[next-2].Index
				prevLogTerm = rf.logs[next-2].Term
			}
			req := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []*Log{},
				LeaderCommit: rf.commitIndex,
			}

			go rf.sendAppendEntriesToPeer(ctx, req, server)
			rf.mu.Unlock()

		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}

}

func (rf *Raft) sendAppendEntriesToPeer(ctx context.Context, req *AppendEntriesArgs, server int) {
	reply := AppendEntriesReply{}

	select {
	case <-ctx.Done():
		return
	default:
	}

	if ok := rf.sendAppendEntries(ctx, server, req, &reply); !ok {
		log.Printf("%d failed rpc request %v to server %d for append entry", rf.me, req, server)
		//retry
		select {
		case <-ctx.Done():
			return
		//TODO: why does sleeping here makes it work?
		case <-time.After(3000 * time.Millisecond):
			// if not heartbeat, retry
			if len(req.Entries) != 0 {
				// go rf.sendAppendEntriesToPeer(ctx, req, server)
			}
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > req.Term {
		log.Printf("%d received an old reply\n", rf.me)
		return

	}

	if rf.currentTerm > reply.Term {
		log.Printf("%d received an old reply\n", rf.me)
		return
	}

	// update term
	if reply.Term > rf.currentTerm {
		rf.handleEvent(context.TODO(), HIGHER_TERM_FOUND, reply.Term)
		// stepdown
		return
	}

	if reply.Success {
		log.Printf("%d => %d append Entry request to was successful\n", rf.me, server)
		log.Printf("%d matchIndex before %v, nextIndex before %v", rf.me, rf.matchIndex, rf.nextIndex)
		// prev := rf.matchIndex[server]
		rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// if prev != rf.matchIndex[server] {
		log.Printf("%d matchIndex updated to %v, nextIndex updated to %v", rf.me, rf.matchIndex, rf.nextIndex)
		//TODO: tryapply only when changed
		// go rf.tryApply()
		// }
	} else {
		rf.nextIndex[server]--

		//retry
		select {
		case <-ctx.Done():
			return
		default:
			entries := make([]*Log, 0)

			next := rf.nextIndex[server]
			entries = append(entries, rf.logs[next-1:]...)

			var prevLogIndex int
			var prevLogTerm int

			if next == 1 {
				prevLogIndex = 0
				prevLogTerm = -1
			} else {
				prevLogIndex = rf.logs[next-2].Index
				prevLogTerm = rf.logs[next-2].Term
			}
			req := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      entries,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntriesToPeer(ctx, req, server)
		}
	}
}

func (rf *Raft) tryApply(ctx context.Context) {
	log.Printf("%d try apply", rf.me)
	log.Printf("%d grabbed lock in apply", rf.me)
	//write lock
	for {
		rf.mu.Lock()
		for N := rf.commitIndex + 1; N <= len(rf.logs); N++ {
			count := 1 // 1 for leader
			log.Printf("%d N: %d, matchIndex: %v", rf.me, N, rf.matchIndex)
			for idx := range rf.peers {
				if rf.matchIndex[idx] >= N {
					count++
				}
			}
			log.Printf("%d N: %d, matchIndex: %v, count %d", rf.me, N, rf.matchIndex, count)
			if count > len(rf.peers)/2 && rf.logs[N-1].Term == rf.currentTerm {
				rf.commitIndex = N
				log.Printf("%d found a suitable N: %d", rf.me, N)
			}
		}
		rf.mu.Unlock()
		rf.tryCommit()
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (rf *Raft) tryCommit() {
	log.Printf("%d trying to grab lock for commit", rf.me)
	rf.mu.Lock()
	res := make([]ApplyMsg, 0)
	//write lock
	commitIndex := rf.commitIndex
	log.Printf("%d trying to commit: %d, %d", rf.me, rf.lastApplied, rf.commitIndex)
	log.Printf("%d trying to commit: %v", rf.me, rf.logs)
	for rf.lastApplied < commitIndex {
		rf.lastApplied++
		l := rf.logs[rf.lastApplied-1]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      l.Command,
			CommandIndex: l.Index,
		}
		log.Printf("%d trying to commit %v", rf.me, msg)
		res = append(res, msg)
	}

	rf.mu.Unlock()
	go func() {
		for _, val := range res {
			rf.applyCh <- val
		}
	}()
	log.Printf("%d finished commit", rf.me)
}

func (rf *Raft) sendAppendEntries(ctx context.Context,
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {

	log.Printf("%d send append entry to %d, %v\n", rf.me, server, *args)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	log.Printf("%d received reply: %t from %d", rf.me, reply.Success, server)
	return ok
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

	rf.heartbeatCh = make(chan struct{})
	rf.voteRequestCh = make(chan struct{})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	ctx, cancelFunc := context.WithCancel(context.Background())
	rf.mainCancelFunc = cancelFunc
	rf.leaderCancelFunc = func() {}

	rf.mu.Lock()
	rf.handleEvent(ctx, STARTED, 0)
	rf.mu.Unlock()

	return rf
}

func (rf *Raft) startElection(ctx context.Context) {
	// TODO: cancel election if receiving heartbeat
	log.Printf("%d: starting election\n", rf.me)
	rf.mu.Lock()
	// increment current term
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me

	lastLogIndex := len(rf.logs)
	var lastLogTerm int

	if lastLogIndex == 0 {
		lastLogTerm = -1 // a substitue for a long value
	} else {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	replyCh := rf.requestVoteToPeers(ctx, &req)

	votes := 1 // includes my vote
	// for each incoming vote replies
	for reply := range replyCh {
		rf.mu.Lock()

		if rf.currentTerm > req.Term {
			log.Printf("%d received an old reply, ignore it.\n", rf.me)
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.handleEvent(context.TODO(), HIGHER_TERM_FOUND, reply.Term)
		}
		rf.mu.Unlock()

		if reply.VoteGranted {
			votes++
		}
		// if we have recieved majority of votes, we don't have to wait for rest of replies
		// we leave the replyCh open, so that the broadcasting go routine can close it
		if votes > len(rf.peers)/2 {
			log.Printf("%d recieved %d / %d votes\n", rf.me, votes, len(rf.peers))
			rf.mu.Lock()
			rf.handleEvent(context.TODO(), MAJOR_VOTES_RECEIVED, 0)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) requestVoteToPeers(ctx context.Context, req *RequestVoteArgs) chan *RequestVoteReply {

	// we actually need rf.peers - 1, but this makes indexing easier
	replies := make([]RequestVoteReply, len(rf.peers))
	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)

	eg, _ := errgroup.WithContext(ctx)
	for idx := range rf.peers {
		server := idx
		if server == rf.me {
			continue
		}
		eg.Go(func() error {
			if ok := rf.sendRequestVote(ctx, server, req, &replies[server]); !ok {
				replies[server] = RequestVoteReply{VoteGranted: false}
				log.Printf("%d failed rpc request for request vote\n", rf.me)
			}
			replyCh <- &replies[server]
			return nil
		})
	}

	go func() {
		defer close(replyCh)
		eg.Wait()
		log.Println("got all replies from rpc request for request vote")
	}()

	return replyCh
}

func init() {
	log.SetOutput(ioutil.Discard)
	log.SetFlags(0)
}
