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

	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	ElectionTimeOutMin = 300
	ElectionTimeOutMax = ElectionTimeOutMin + 100
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
	// persistent states
	votedFor    int
	currentTerm int
	logs        []*Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	// notifies that a heartbeat from the leader has arrived
	heartbeatCh   chan struct{}
	voteRequestCh chan struct{}

	applyCh chan ApplyMsg

	lastNewEntryIndex int

	mainContext      context.Context
	mainCancelFunc   func()
	leaderCancelFunc func()
	leaderContext    context.Context

	candidateContext    context.Context
	candidateCancleFunc func()
}

type Data struct {
	VotedFor    int
	CurrentTerm int
	Logs        []*Log
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
			go rf.tryApplyToClient(ctx)
		}
	case FOLLOWER:
		if e == ELECTION_TIMEOUTED {
			log.Printf("%d: FOLLOWER => CANDIDATE\n", rf.me)
			rf.mu.Lock()
			rf.status = CANDIDATE
			rf.candidateContext, rf.candidateCancleFunc = context.WithCancel(ctx)
			go rf.startElection(rf.candidateContext)
			rf.mu.Unlock()
		} else if e == HIGHER_TERM_FOUND {
			log.Printf("%d: FOLLOWER => FOLLOWER\n", rf.me)
			rf.handleHigherTermFound(data)
		}
	case CANDIDATE:
		if e == MAJOR_VOTES_RECEIVED {
			log.Printf("%d: CANDIDATE => LEADER\n", rf.me)
			rf.status = LEADER
			rf.candidateCancleFunc()

			rf.nextIndex = make([]int, len(rf.peers))
			for idx := range rf.nextIndex {
				if len(rf.logs) == 0 {
					rf.nextIndex[idx] = rf.snapshotIndex + 1
				} else {
					rf.nextIndex[idx] = rf.logs[len(rf.logs)-1].Index + 1
				}
			}

			rf.matchIndex = make([]int, len(rf.peers))

			rf.leaderContext, rf.leaderCancelFunc = context.WithCancel(ctx)

			// send heart beat as new leader
			go rf.sendHeartBeat(rf.leaderContext)
			go rf.tryUpdateCommitIndex(rf.leaderContext)
			go rf.periodicAgreement(rf.leaderContext)

			// start begin entries for once
			// for server := range rf.peers {
			// 	if server != rf.me {
			// 		go rf.beginEntriesAgreement(server)
			// 	}
			// }
		} else if e == CURRENT_LEADER_FOUND {
			log.Printf("%d: CANDIDATE => FOLLOWER\n", rf.me)
			rf.candidateCancleFunc()
			// rf.followerContext = nil
			rf.status = FOLLOWER
		} else if e == HIGHER_TERM_FOUND {
			log.Printf("%d: CANDIDATE => FOLLOWER\n", rf.me)
			rf.candidateCancleFunc()
			// rf.followerContext = nil
			rf.handleHigherTermFound(data)
		} else if e == ELECTION_TIMEOUTED {
			log.Printf("%d: CANDIDATE => CANDIDATE\n", rf.me)
			go rf.startElection(rf.candidateContext)
		}
	case LEADER:
		if e == HIGHER_TERM_FOUND {
			log.Printf("%d: LEADER => FOLLOWER\n", rf.me)
			rf.leaderCancelFunc()
			rf.handleHigherTermFound(data)
		} else if e == CURRENT_LEADER_FOUND {
			log.Printf("%d: LEADER => FOLLOWER\n", rf.me)
			rf.leaderCancelFunc()
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
	rf.persist()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Data{CurrentTerm: rf.currentTerm, VotedFor: rf.votedFor, Logs: rf.logs})
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var buf Data
	// var yyy
	if err := d.Decode(&buf); err != nil {
		log.Printf("%v", err.Error())
	} else {
		rf.currentTerm = buf.CurrentTerm
		rf.votedFor = buf.VotedFor
		rf.logs = buf.Logs
		// log.Printf("read persist..!")
		// log.Printf("%v", buf)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.snapshot = snapshot

	toKeep := index + 1

	rf.snapshotTerm = rf.logs[index-1-rf.snapshotIndex].Term
	//if index is 2
	rf.logs = rf.logs[toKeep-rf.snapshotIndex-1:]

	rf.snapshotIndex = index

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
	Term                  int
	Success               bool
	FirstConflictingIndex int
	ConflictingTerm       int
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// InstallSnapshot RPC handler.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	//1. reply immediately if
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.handleEvent(rf.mainContext, HIGHER_TERM_FOUND, args.Term)
		go func() {
			rf.heartbeatCh <- struct{}{}
		}()
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Data{CurrentTerm: rf.currentTerm, VotedFor: rf.votedFor, Logs: rf.logs})
	data := w.Bytes()
	// save snapshot
	rf.snapshot = args.Data
	rf.persister.SaveStateAndSnapshot(data, args.Data)

	fmt.Println(rf.logs)
	fmt.Println(args.LastIncludedIndex)
	fmt.Println(args.LastIncludedTerm)
	if len(rf.logs) > args.LastIncludedIndex-rf.snapshotIndex && rf.logs[args.LastIncludedIndex-1-rf.snapshotIndex].Term == args.LastIncludedTerm {
		//retain log entries following it
		rf.logs = rf.logs[args.LastIncludedIndex-rf.snapshotIndex:]
		rf.mu.Unlock()
		return
	} else {
		rf.logs = make([]*Log, 0)
	}

	rf.mu.Unlock()
	// 8. reset state machine using snapshot contents
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
}

//
// RequestVote RPC handler.
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
		rf.handleEvent(rf.mainContext, HIGHER_TERM_FOUND, args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// log.Printf("%d can vote for %d, check logs %v", rf.me, args.CandidateId, rf.logs)
		log.Printf("%d can vote for, args: %v", rf.me, args)
		var lastLogIndex int
		var lastLogTerm int
		if len(rf.logs) == 0 {
			lastLogIndex = rf.snapshotIndex
			lastLogTerm = rf.snapshotTerm
		} else {
			lastLogIndex = rf.logs[len(rf.logs)-1].Index
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			log.Printf("%d voted for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
			go func() {
				rf.voteRequestCh <- struct{}{}
			}()
		} else {
			log.Printf("%d did not vote for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.votedFor = -1
			rf.persist()
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

func (rf *Raft) requestInstallSnapshot(ctx context.Context, server int) {
	rf.mu.Lock()

	args := InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Offset:            0,
		Data:              rf.snapshot,
		Done:              true,
	}

	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	go func() {
		rf.sendInstallSnapshot(ctx, server, &args, &reply)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.handleEvent(ctx, HIGHER_TERM_FOUND, reply.Term)
		}
		defer rf.mu.Lock()
	}()

}

// sendInstallSnapshot RPC
func (rf *Raft) sendInstallSnapshot(ctx context.Context,
	server int,
	args *InstallSnapshotRequest,
	reply *InstallSnapshotReply) bool {

	log.Printf("%d send install snapshot to %d\n", rf.me, server)

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
		go func() {
			rf.heartbeatCh <- struct{}{}
		}()
		rf.handleEvent(rf.mainContext, CURRENT_LEADER_FOUND, 0)
	}

	if args.Term > rf.currentTerm {
		rf.handleEvent(rf.mainContext, HIGHER_TERM_FOUND, args.Term)
	}

	log.Printf("%d, args: %v", rf.me, args)
	// log.Printf("%d's current logs: %v, pli: %d", rf.me, rf.logs, args.PrevLogIndex)

	reply.Term = rf.currentTerm

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("reply false because %d has higher term %d than %d", rf.me, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.logs)+rf.snapshotIndex {
		// fmt.Printf("%d: this is true!!!", rf.me)
		reply.Success = false
		return
	}

	// 2. if log doesn't contain an entry at prevLogIndex...
	if args.PrevLogIndex > rf.snapshotIndex && rf.logs[args.PrevLogIndex-rf.snapshotIndex-1].Term != args.PrevLogTerm {
		//
		reply.ConflictingTerm = rf.logs[args.PrevLogIndex-rf.snapshotIndex-1].Term
		for _, val := range rf.logs {
			if val.Term == reply.ConflictingTerm {
				reply.FirstConflictingIndex = val.Index + 1
				break
			}
		}
		reply.Success = false

		return
	}

	reply.Success = true

	//FIXME!
	for _, val := range args.Entries {
		if val.Index > len(rf.logs)+rf.snapshotIndex {
			break //FIXME?
		}
		// same index, but diffrent terms
		if rf.logs[val.Index-1-rf.snapshotIndex].Term != val.Term {
			// delete the existing entry and all that follow it
			// by only retaining the entries before it
			rf.logs = rf.logs[:val.Index-1-rf.snapshotIndex]
			rf.persist()
			break
		}
	}

	for _, val := range args.Entries {
		if val.Index > len(rf.logs)+rf.snapshotIndex {
			log.Printf("%d appending %v to %v", rf.me, val, rf.logs)
			rf.logs = append(rf.logs, val)
			rf.persist()
			rf.lastNewEntryIndex = val.Index
			log.Printf("%d lastNewEntryINdex %d", rf.me, rf.lastNewEntryIndex)
		}
	}

	log.Printf("%d added args.Entries %v to its logs %v", rf.me, args.Entries, rf.logs)

	if args.LeaderCommit > rf.commitIndex {
		log.Printf("%d leader commit updated %d, %d, %d", rf.me, args.LeaderCommit, rf.commitIndex, rf.lastNewEntryIndex)
		// prev := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, rf.lastNewEntryIndex)
		log.Printf("%d new commit index %d", rf.me, rf.commitIndex)
		// if prev != rf.commitIndex {
		// go rf.tryCommit()
		// }
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

	index := len(rf.logs) + rf.snapshotIndex + 1
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
	rf.persist()

	rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.beginEntriesAgreement(rf.leaderContext, server)
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

	var lastLogIndex int
	if len(rf.logs) == 0 {
		lastLogIndex = rf.snapshotIndex
	} else {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
	}

	if lastLogIndex >= rf.nextIndex[server] {
		entries := make([]*Log, 0)

		next := rf.nextIndex[server]
		// fmt.Println(next)
		if next-rf.snapshotIndex >= 1 {
			entries = append(entries, rf.logs[next-rf.snapshotIndex-1:]...)
		} else if len(rf.logs) > 0 && rf.logs[0].Index > next {
			log.Printf("should send install snapshot")
			log.Printf("%d's log: %v", rf.me, rf.logs)
			log.Printf("next: %d", next)
			go rf.requestInstallSnapshot(ctx, server)
			return
		}

		var prevLogIndex int
		var prevLogTerm int

		if next-rf.snapshotIndex <= 1 {
			prevLogIndex = rf.snapshotIndex
			prevLogTerm = rf.snapshotTerm
		} else {
			prevLogIndex = rf.logs[next-rf.snapshotIndex-2].Index
			prevLogTerm = rf.logs[next-rf.snapshotIndex-2].Term
		}
		req := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      entries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		if len(req.Entries) > 0 {
			go rf.sendAppendEntriesToPeer(ctx, req, server)
		}
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
	// close(rf.applyCh)
	// close(rf.heartbeatCh)
	// close(rf.voteRequestCh)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(ctx context.Context) {
	for {
		r := ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
		t := time.Duration(r) * time.Millisecond
		// r can be infinite if leader
		// rf.mu.Lock()
		// 300 years
		// t = time.Duration(math.MaxInt64)
		// log.Printf("setting a large timer %d", t)
		// }
		// rf.mu.Unlock()

		select {
		case <-rf.heartbeatCh:
			// log.Printf("%d received heartbeat before election timeout\n", rf.me)
		case <-rf.voteRequestCh:
			// log.Printf("%d received voterequest before election timeout\n", rf.me)
		case <-time.After(t):
			// TODO: cancel election if receiving heartbeat
			// rf.mu.Lock()
			go rf.handleEvent(ctx, ELECTION_TIMEOUTED, 0)
			// rf.mu.Unlock()
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
			// fmt.Printf("next %d\n", next)
			// fmt.Printf("index %d\n", rf.snapshotIndex)
			// fmt.Printf("logs %v\n", rf.logs)
			if next-rf.snapshotIndex <= 1 {
				prevLogIndex = rf.snapshotIndex
				prevLogTerm = rf.snapshotTerm
			} else {
				prevLogIndex = rf.logs[next-rf.snapshotIndex-2].Index
				prevLogTerm = rf.logs[next-rf.snapshotIndex-2].Term
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
		case <-time.After(150 * time.Millisecond):
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
		// log.Printf("%d failed rpc request %v to server %d for append entry", rf.me, req, server)
		return
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
		rf.handleEvent(rf.mainContext, HIGHER_TERM_FOUND, reply.Term)
		// stepdown
		return
	}

	if reply.Success {
		log.Printf("%d => %d append Entry request to was successful\n", rf.me, server)
		log.Printf("%d matchIndex before %v, nextIndex before %v", rf.me, rf.matchIndex, rf.nextIndex)
		// prev := rf.matchIndex[server]
		rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
		// fmt.Println("here")
		// fmt.Println(rf.matchIndex[server])
		// rf.nextIndex[server] = rf.matchIndex[server] + 1 + rf.snapshotIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// if prev != rf.matchIndex[server] {
		log.Printf("%d matchIndex updated to %v, nextIndex updated to %v", rf.me, rf.matchIndex, rf.nextIndex)
		//TODO: tryapply only when changed
		// go rf.tryApply()
		// }
	} else {
		// if rf.logs[reply.FirstConflictingIndex-1]
		// WHAT TO DO with term??
		// find Max(firstIndexWithConflictingTerm, reply's firstConflictingIndex)
		// rf.nex
		// rf.nextIndex[server] = Max(reply.FirstConflictingIndex, 1)
		// var x int
		x := rf.snapshotIndex + 1
		for _, val := range rf.logs {
			if val.Term == reply.ConflictingTerm {
				x = val.Index
			}
		}
		rf.nextIndex[server] = Max(x, reply.FirstConflictingIndex)
		// Max might be unecessary
		// rf.nextIndex[server] = rf.nextIndex[server] - 1
		// TODO: we need optimization to solve Test (2C): Figure 8 (unreliable) ...
		// rf.nextIndex[server] = Max(rf.nextIndex[server]-1, 1)

		//retry
		entries := make([]*Log, 0)

		next := rf.nextIndex[server]
		if next-rf.snapshotIndex >= 1 {
			entries = append(entries, rf.logs[next-1-rf.snapshotIndex:]...)
		} else if len(rf.logs) > 0 && rf.logs[0].Index > next {
			go rf.requestInstallSnapshot(ctx, server)
			return
		}

		var prevLogIndex int
		var prevLogTerm int

		if next-rf.snapshotIndex <= 1 {
			prevLogIndex = rf.snapshotIndex
			prevLogTerm = rf.snapshotTerm
		} else {
			prevLogIndex = rf.logs[next-rf.snapshotIndex-2].Index
			prevLogTerm = rf.logs[next-rf.snapshotIndex-2].Term
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

// even without Start, we have to try and sync other followers with leader's logs
func (rf *Raft) periodicAgreement(ctx context.Context) {

	for {

		eg, _ := errgroup.WithContext(ctx)
		for idx := range rf.peers {
			server := idx
			if server == rf.me {
				continue
			}
			eg.Go(func() error {
				rf.beginEntriesAgreement(ctx, server)
				return nil
			})
		}
		eg.Wait()
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}

}

func (rf *Raft) tryUpdateCommitIndex(ctx context.Context) {
	log.Printf("%d try updateCommitIndex", rf.me)
	log.Printf("%d grabbed lock in updateCommitIndex", rf.me)
	//write lock
	for {
		rf.mu.Lock()
		for N := rf.commitIndex + 1; N <= len(rf.logs)+rf.snapshotIndex; N++ {
			count := 1 // 1 for leader
			log.Printf("%d N: %d, matchIndex: %v", rf.me, N, rf.matchIndex)
			for idx := range rf.peers {
				if idx != rf.me && rf.matchIndex[idx] >= N {
					count++
				}
			}
			log.Printf("%d N: %d, matchIndex: %v, count %d", rf.me, N, rf.matchIndex, count)
			if count > len(rf.peers)/2 && rf.logs[N-1-rf.snapshotIndex].Term == rf.currentTerm {
				rf.commitIndex = N
				log.Printf("%d found a suitable N: %d", rf.me, N)
			}
		}
		rf.mu.Unlock()
		//tryCommit should be ran in its own designated goroutine; not called by others
		// if tryCommit is called by other function as goroutines; the order of commit is not fixed
		// rf.tryCommit()
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (rf *Raft) tryApplyToClient(ctx context.Context) {
	log.Printf("%d trying to grab lock for apply", rf.me)
	for {
		res := make([]ApplyMsg, 0)
		rf.mu.Lock()
		//write lock
		commitIndex := rf.commitIndex
		log.Printf("%d trying to apply: %d, %d", rf.me, rf.lastApplied, rf.commitIndex)
		// log.Printf("%d trying to commit: %v", rf.me, rf.logs[rf.lastApplied-1:rf.commitIndex-1])
		for rf.lastApplied < commitIndex {
			rf.lastApplied++
			l := rf.logs[rf.lastApplied-1-rf.snapshotIndex]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: l.Index,
			}
			log.Printf("%d trying to apply %v", rf.me, msg)
			res = append(res, msg)
		}
		rf.mu.Unlock()
		if len(res) > 0 {
			log.Printf("%d applying: %v", rf.me, res)
		}
		for _, val := range res {
			rf.applyCh <- val
		}
		log.Printf("%d finished apply", rf.me)

		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (rf *Raft) sendAppendEntries(ctx context.Context,
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {

	// log.Printf("%d send append entry to %d, %v\n", rf.me, server, *args)

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

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.heartbeatCh = make(chan struct{})
	rf.voteRequestCh = make(chan struct{})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	ctx, cancelFunc := context.WithCancel(context.Background())
	rf.mainContext = ctx
	rf.mainCancelFunc = cancelFunc
	rf.leaderCancelFunc = func() {}

	rf.snapshotIndex = 0
	rf.snapshotTerm = -1

	rf.mu.Lock()
	rf.handleEvent(ctx, STARTED, 0)
	rf.mu.Unlock()

	return rf
}

func (rf *Raft) startElection(ctx context.Context) {
	log.Printf("%d: starting election\n", rf.me)
	rf.mu.Lock()
	// increment current term
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me
	rf.persist()

	var lastLogIndex int
	var lastLogTerm int
	if len(rf.logs)-rf.snapshotIndex == 0 {
		lastLogIndex = 0
		lastLogTerm = -1
	} else {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
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
			rf.handleEvent(rf.mainContext, HIGHER_TERM_FOUND, reply.Term)
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
			rf.handleEvent(rf.mainContext, MAJOR_VOTES_RECEIVED, 0)
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
				// log.Printf("%d failed rpc request for request vote\n", rf.me)
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
