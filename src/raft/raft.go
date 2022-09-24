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

type StartRPC struct {
	command interface{}
	replyCh chan *struct {
		index    int
		term     int
		isLeader bool
	}
}

type RequestVoteRPC struct {
	args    *RequestVoteArgs
	replyCh chan *RequestVoteReply
}

type InstallSnapshotRPC struct {
	args    *InstallSnapshotRequest
	replyCh chan *InstallSnapshotReply
}

type AppendEntriesRPC struct {
	args    *AppendEntriesArgs
	replyCh chan *AppendEntriesReply
}

// holds append entries reply with meta
type AppendEntriesTuple struct {
	reply *AppendEntriesReply

	args   *AppendEntriesArgs
	server int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// protects status and currentTerm together
	// since this two field can be accessed outside the main thread
	stateLock   sync.Mutex
	status      status // Leader, Follower, Candidate
	currentTerm int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent states
	votedFor int

	logs []*Log

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

	appendEntriesCh chan *AppendEntriesRPC

	appendEntriesReplyCh chan *AppendEntriesTuple

	requestVoteCh chan *RequestVoteRPC

	InstallSnapshotCh chan *struct {
		*InstallSnapshotRequest
		*InstallSnapshotReply
	}

	applyCh   chan ApplyMsg
	commandCh chan *StartRPC

	lastNewEntryIndex int

	mainContext    context.Context
	mainCancelFunc func()
}

type Data struct {
	VotedFor    int
	CurrentTerm int
	Logs        []*Log
}

// only called from the main thread
func (rf *Raft) handleHigherTermFound(term int) {
	// update term

	rf.setState(term, FOLLOWER)
	// since we updated term, we reset votedFor; we haven't voted for anyone in this new term (yet)
	rf.votedFor = -1
	rf.persist()
}

// TODO: lock using a state lock
func (rf *Raft) GetState() (int, bool) {
	log.Println("GetState called")

	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()

	log.Printf("%d: currentTerm %d, status %s", rf.me, rf.currentTerm, rf.status)

	return rf.currentTerm, rf.status == LEADER
}

func (rf *Raft) setState(term int, status status) {
	log.Println("GetState called")
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()

	rf.currentTerm = term
	rf.status = status
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
	e.Encode(Data{CurrentTerm: rf.getTerm(), VotedFor: rf.votedFor, Logs: rf.logs})
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
		rf.setTerm(buf.CurrentTerm)
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

	//TODO: FIXME
	reply.Term = rf.getTerm()

	//1. reply immediately if
	if args.Term < rf.getTerm() {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.getTerm() {
		// rf.handleEvent(rf.mainContext, HIGHER_TERM_FOUND, args.Term)
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
	replyCh := make(chan *RequestVoteReply)

	// send a message to the main thread to process this request
	rf.requestVoteCh <- &RequestVoteRPC{
		args:    args,
		replyCh: replyCh,
	}

	// wait for main loop's result
	res := <-replyCh

	// return reply
	reply.Term = res.Term
	reply.VoteGranted = res.VoteGranted
}

// should only be called from the main thread
func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("%d received a vote request from %d\n", rf.me, args.CandidateId)

	if args.Term > rf.getTerm() {
		rf.handleHigherTermFound(args.Term)
	}
	// 1. Reply false if term < currentTerm
	currentTerm := rf.getTerm()
	if args.Term < currentTerm {
		log.Printf("%d not voting for %d\n", rf.me, args.CandidateId)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}

	currentTerm = rf.getTerm()

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// log.Printf("%d can vote for %d, check logs %v", rf.me, args.CandidateId, rf.logs)
		log.Printf("%d can vote for %d, args: %v", rf.me, args.CandidateId, args)
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
			reply.Term = currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			log.Printf("%d did not vote for %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = currentTerm
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
			// rf.handleEvent(ctx, HIGHER_TERM_FOUND, reply.Term)
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
	replyCh := make(chan *AppendEntriesReply)

	// send a message to the main thread to process this request
	rf.appendEntriesCh <- &AppendEntriesRPC{
		args:    args,
		replyCh: replyCh,
	}

	// wait for main loop's result
	res := <-replyCh

	// return reply
	reply.ConflictingTerm = res.ConflictingTerm
	reply.FirstConflictingIndex = res.FirstConflictingIndex
	reply.Success = res.Success
	reply.Term = res.Term
}

// only called from the main thread
func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// TODO: should this code check if the appendEntry request is from a valid leader? <- YES
	// received heartbeat
	if args.Term > rf.getTerm() {
		rf.handleHigherTermFound(args.Term)
	}

	reply.Term = rf.getTerm()
	reply.Success = false

	//???
	if args.Term == rf.getTerm() {
		log.Printf("%d received valid heartbeat from %d\n", rf.me, args.LeaderId)
		// rf.handleHigherTermFound(args.Term)
		// received heartbeat
		// go func() {
		// rf.heartbeatCh <- struct{}{}
		// }()
		// rf.handleEvent(rf.mainContext, CURRENT_LEADER_FOUND, 0)
		// rf.handleHigherTermFound(args.Term)
		rf.setStatus(FOLLOWER)
	}

	log.Printf("%d, args: %v", rf.me, args)
	// log.Printf("%d's current logs: %v, pli: %d", rf.me, rf.logs, args.PrevLogIndex)

	// 1. reply false if term < currentTerm
	if args.Term < rf.getTerm() {
		log.Printf("reply false because %d has higher term %d than %d", rf.me, rf.currentTerm, args.LeaderId)
		return
	}

	if args.PrevLogIndex > len(rf.logs)+rf.snapshotIndex {
		// fmt.Printf("%d: this is true!!!", rf.me)
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

	if args.LeaderCommit > 0 && args.LeaderCommit > rf.commitIndex {
		log.Printf("%d leader commit updated %d, %d, %d", rf.me, args.LeaderCommit, rf.commitIndex, rf.lastNewEntryIndex)
		prev := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, rf.lastNewEntryIndex)
		log.Printf("%d new commit index %d", rf.me, rf.commitIndex)
		if prev != rf.commitIndex {
			rf.applyToClient(rf.mainContext)
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
//TODO: make this to a channel, with command
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	replyCh := make(chan *struct {
		index    int
		term     int
		isLeader bool
	})
	startCommand := &StartRPC{command: command, replyCh: replyCh}

	rf.commandCh <- startCommand

	reply := <-replyCh

	return reply.index, reply.term, reply.isLeader
}

// should only be called from the main thread, should return asap
func (rf *Raft) handleCommand(command interface{}) (int, int, bool) {

	index := len(rf.logs) + rf.snapshotIndex + 1
	term, isLeader := rf.GetState()

	if !isLeader {
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

	return index, term, isLeader
}

// should be called in the main thread
func (rf *Raft) singleAppendEntries(ctx context.Context, server int) <-chan *AppendEntriesTuple {
	reply := AppendEntriesReply{}

	var lastLogIndex int
	if len(rf.logs) == 0 {
		lastLogIndex = rf.snapshotIndex
	} else {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
	}

	if lastLogIndex >= rf.nextIndex[server] {
		entries := make([]*Log, 0)

		next := rf.nextIndex[server]
		if next-rf.snapshotIndex >= 1 {
			entries = append(entries, rf.logs[next-1-rf.snapshotIndex:]...)
		} else if len(rf.logs) > 0 && rf.logs[0].Index > next {
			// go rf.requestInstallSnapshot(ctx, server)
			// continue
			// return
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
			Term:         rf.getTerm(),
			LeaderId:     rf.me,
			Entries:      entries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}

		go func() {
			if len(req.Entries) <= 0 {
				return
			}
			if ok := rf.sendAppendEntries(ctx, server, req, &reply); !ok {
				log.Printf("%d failed rpc request for appendEntry\n", rf.me)
				reply = AppendEntriesReply{}
			}
			rf.appendEntriesReplyCh <- &AppendEntriesTuple{reply: &reply, args: req, server: server}
		}()
	}

	return rf.appendEntriesReplyCh
}

// should be called in the main thread
func (rf *Raft) broadcastAppendEntries(ctx context.Context) <-chan *AppendEntriesTuple {

	log.Printf("%d, beginEntriesAgreement NextIndex: %v", rf.me, rf.nextIndex)

	// we actually need rf.peers - 1, but this makes indexing easier
	// replies := make([]AppendEntriesReply, len(rf.peers))
	// replyCh := make(chan *AppendEntriesReply, len(rf.peers)-1)

	for idx := range rf.peers {
		server := idx
		if server == rf.me {
			continue
		}

		rf.singleAppendEntries(ctx, server)
	}
	return rf.appendEntriesReplyCh
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

// only called in the main thread
func (rf *Raft) sendHeartBeat(ctx context.Context) <-chan *AppendEntriesTuple {
	log.Printf("%d: sending heartbeat\n", rf.me)
	return rf.sendEmptyAppendEntriesToPeers(ctx)
}

func (rf *Raft) sendEmptyAppendEntriesToPeers(ctx context.Context) <-chan *AppendEntriesTuple {

	// we actually need rf.peers - 1, but this makes indexing easier
	replies := make([]AppendEntriesReply, len(rf.peers))
	// replyCh := make(chan *AppendEntriesReply, len(rf.peers)-1)
	term := rf.getTerm()

	eg, _ := errgroup.WithContext(ctx)
	for idx := range rf.peers {
		server := idx
		if server == rf.me {
			continue
		}

		next := rf.nextIndex[server]

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
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      []*Log{},
			LeaderCommit: rf.commitIndex,
		}
		eg.Go(func() error {
			if ok := rf.sendAppendEntries(ctx, server, req, &replies[server]); !ok {
				log.Printf("%d failed rpc request for appendEntry\n", rf.me)
				replies[server] = AppendEntriesReply{}
			}
			rf.appendEntriesReplyCh <- &AppendEntriesTuple{reply: &replies[server], args: req, server: server}
			return nil
		})
	}

	go func() {
		// defer close(replyCh)
		eg.Wait()
		log.Println("got all replies from rpc request for request vote")
	}()

	return rf.appendEntriesReplyCh
}

// called only in the main thread
func (rf *Raft) updateCommitIndex(ctx context.Context) {
	log.Printf("%d try updateCommitIndex", rf.me)

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
}

// called only in the main thread
func (rf *Raft) applyToClient(ctx context.Context) {
	res := make([]ApplyMsg, 0)
	commitIndex := rf.commitIndex
	for commitIndex > rf.lastApplied {
		rf.lastApplied++
		l := rf.logs[rf.lastApplied-1-rf.snapshotIndex]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      l.Command,
			CommandIndex: l.Index,
		}
		res = append(res, msg)
	}
	for _, val := range res {
		rf.applyCh <- val
	}
	if len(res) > 0 {
		log.Printf("----------------------------------------------")
		log.Printf("%d applied %v", rf.me, res)
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

// run the main thread that handles leadership and RPC requests.
func (rf *Raft) run() {
	log.Printf("%d starting the main loop", rf.me)

	// every one is follower in the beginning
	rf.setStatus(FOLLOWER)

	for {
		select {
		case <-rf.mainContext.Done():
			return
		default:
		}

		switch rf.getStatus() {
		case FOLLOWER:
			rf.runAsFollower(rf.mainContext)
		case CANDIDATE:
			rf.runAsCandidate(rf.mainContext)
		case LEADER:
			rf.runAsLeader(rf.mainContext)
		}
	}
}

//TODO: atomic read of rf.status
func (rf *Raft) getStatus() status {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()

	return rf.status
}

//TODO: atomic update of rf.status OR, a rwlock
func (rf *Raft) setStatus(s status) {
	rf.stateLock.Lock()
	rf.status = s
	rf.stateLock.Unlock()

}

//TODO: atomic read of rf.term
func (rf *Raft) getTerm() int {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()

	return rf.currentTerm
}

//TODO: atomic read of rf.term
func (rf *Raft) setTerm(term int) {
	rf.stateLock.Lock()
	rf.currentTerm = term
	rf.stateLock.Unlock()
}

// runFollower runs the main loop while in the follower state.
func (rf *Raft) runAsFollower(ctx context.Context) {
	log.Printf("%d running in the main loop as follower", rf.me)

	// things to do when we become a follower
	r := ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
	t := time.Duration(r) * time.Millisecond
	timeoutCh := time.After(t)

	// applyTimeOutCh := time.After(10 * time.Millisecond)

	for rf.getStatus() == FOLLOWER {
		select {
		case req := <-rf.requestVoteCh:
			reply := &RequestVoteReply{}
			rf.handleRequestVote(req.args, reply)
			req.replyCh <- reply

			// reset timer?
			r = ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
			t = time.Duration(r) * time.Millisecond
			timeoutCh = time.After(t)

		case req := <-rf.appendEntriesCh:
			reply := &AppendEntriesReply{}
			rf.handleAppendEntries(req.args, reply)
			req.replyCh <- reply

			// reset timer
			r = ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
			t = time.Duration(r) * time.Millisecond
			timeoutCh = time.After(t)

		case req := <-rf.commandCh:
			index, term, isLeader := rf.handleCommand(req.command)

			req.replyCh <- &struct {
				index    int
				term     int
				isLeader bool
			}{
				index,
				term,
				isLeader,
			}

		// case <-applyTimeOutCh:
		// rf.applyToClient(ctx)

		// applyTimeOutCh = time.After(10 * time.Millisecond)

		// electionTimeOut expired
		case <-timeoutCh:
			rf.setStatus(CANDIDATE)

		case <-ctx.Done():
			return
		}
	}
}

// runCandidate runs the main loop while in the candidate state.
func (rf *Raft) runAsCandidate(ctx context.Context) {
	log.Printf("%d running in the main loop as candidate", rf.me)
	// things to do when we become a candidate

	// things to do when we become a follower
	r := ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
	t := time.Duration(r) * time.Millisecond
	timeoutCh := time.After(t)

	grantedVotes := 1 // includes my vote
	votesNeeded := len(rf.peers) / 2

	voteCh := rf.startElection(rf.mainContext)
	// applyTimeOutCh := time.After(10 * time.Millisecond)

	for rf.getStatus() == CANDIDATE {
		select {
		case req := <-rf.requestVoteCh:
			reply := &RequestVoteReply{}
			rf.handleRequestVote(req.args, reply)
			req.replyCh <- reply
			close(req.replyCh)

			// reset timer
			r = ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
			t = time.Duration(r) * time.Millisecond
			timeoutCh = time.After(t)
		// instead of waiting for the entire votes, wait for seperate votes
		case vote := <-voteCh:
			// read all
			// TEMP: avoid reading from closed channels
			if vote == nil {
				continue
			}
			// handle vote
			if vote.Term > rf.getTerm() {
				log.Printf("%d found a higher term stepping down\n", rf.me)
				rf.handleHigherTermFound(vote.Term)
				return
			}

			if vote.VoteGranted {
				grantedVotes++
			}
			if grantedVotes > votesNeeded {
				log.Printf("%d recieved %d / %d votes\n", rf.me, grantedVotes, len(rf.peers))
				rf.setStatus(LEADER)
				return
			}

		case req := <-rf.commandCh:
			index, term, isLeader := rf.handleCommand(req.command)

			req.replyCh <- &struct {
				index    int
				term     int
				isLeader bool
			}{
				index,
				term,
				isLeader,
			}

		case req := <-rf.appendEntriesCh:
			reply := &AppendEntriesReply{}
			rf.handleAppendEntries(req.args, reply)
			req.replyCh <- reply
			close(req.replyCh)

			// reset timer
			r = ElectionTimeOutMin + rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin+1)
			t = time.Duration(r) * time.Millisecond
			timeoutCh = time.After(t)

		// case <-applyTimeOutCh:
		// rf.applyToClient(ctx)

		// applyTimeOutCh = time.After(10 * time.Millisecond)

		case <-timeoutCh:
			// return as candidate, which will restart the voting process
			return

		case <-ctx.Done():
			return
		}
	}
}

// runCandidate runs the main loop while in the leader state.
func (rf *Raft) runAsLeader(ctx context.Context) {
	log.Printf("%d running in the main loop as leader", rf.me)

	// things to do when we first become a leader
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		if len(rf.logs) == 0 {
			rf.nextIndex[idx] = rf.snapshotIndex + 1
		} else {
			rf.nextIndex[idx] = rf.logs[len(rf.logs)-1].Index + 1
		}
	}

	rf.matchIndex = make([]int, len(rf.peers))

	// send heart beat as new leader, once
	leaderContext, leaderCancel := context.WithCancel(ctx)
	rf.sendHeartBeat(leaderContext)

	heartbeatTimeOutCh := time.After(150 * time.Millisecond)
	//TODO: temp fix: instead of trying it every loop, do it after responses from followers
	tryUpdateCommitTimeOutCh := time.After(100 * time.Millisecond)

	for rf.getStatus() == LEADER {

		select {
		case req := <-rf.requestVoteCh:
			reply := &RequestVoteReply{}
			rf.handleRequestVote(req.args, reply)
			req.replyCh <- reply
			close(req.replyCh)

		case req := <-rf.commandCh:
			log.Printf("%d received cmd %v as leader", rf.me, req.command)
			index, term, isLeader := rf.handleCommand(req.command)

			req.replyCh <- &struct {
				index    int
				term     int
				isLeader bool
			}{
				index,
				term,
				isLeader,
			}

			rf.broadcastAppendEntries(leaderContext)

		case replyTup := <-rf.appendEntriesReplyCh:
			log.Printf("recieving reply from appned entries")
			reply := replyTup.reply
			req := replyTup.args
			server := replyTup.server
			if rf.getTerm() > reply.Term {
				log.Printf("%d received an old reply\n", rf.me)
				continue
			}
			if rf.getTerm() > req.Term {
				log.Printf("%d received an old reply\n", rf.me)
				continue
			}
			if reply.Term > rf.getTerm() {
				rf.handleHigherTermFound(reply.Term)
				continue
			}
			if reply.Success {
				// update nextIndex and matchIndex for follower
				log.Printf("updating nextIndex and matchIndex for follower")
				log.Printf("before: %v, %v", rf.nextIndex, rf.matchIndex)
				rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				log.Printf("after: %v, %v", rf.nextIndex, rf.matchIndex)
			} else { //retry, after decrementing nextIndex

				// decrement nextIndex
				x := rf.snapshotIndex + 1
				for _, val := range rf.logs {
					if val.Term == reply.ConflictingTerm {
						x = val.Index
					}
				}
				rf.nextIndex[server] = Max(x, reply.FirstConflictingIndex)
				rf.singleAppendEntries(leaderContext, server)
			}

		case <-heartbeatTimeOutCh:
			rf.sendHeartBeat(leaderContext)
			// refresh timer
			heartbeatTimeOutCh = time.After(150 * time.Millisecond)

		// FIXME: remove?
		case <-tryUpdateCommitTimeOutCh:
			rf.updateCommitIndex(leaderContext)
			rf.applyToClient(leaderContext)

			tryUpdateCommitTimeOutCh = time.After(100 * time.Millisecond)

		case req := <-rf.appendEntriesCh:
			reply := &AppendEntriesReply{}
			rf.handleAppendEntries(req.args, reply)
			req.replyCh <- reply

		case <-ctx.Done():
			leaderCancel()
			return
		}
	}
	leaderCancel()
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

	// can buffer up to 1000 commands??
	rf.commandCh = make(chan *StartRPC)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	ctx, cancelFunc := context.WithCancel(context.Background())
	rf.mainContext = ctx
	rf.mainCancelFunc = cancelFunc

	rf.snapshotIndex = 0
	rf.snapshotTerm = -1

	rf.requestVoteCh = make(chan *RequestVoteRPC)
	rf.appendEntriesCh = make(chan *AppendEntriesRPC)
	rf.appendEntriesReplyCh = make(chan *AppendEntriesTuple)

	go rf.run()

	return rf
}

func (rf *Raft) startElection(ctx context.Context) <-chan *RequestVoteReply {
	log.Printf("%d: starting election\n", rf.me)
	rf.setTerm(rf.getTerm() + 1)
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
	req := &RequestVoteArgs{
		Term:         rf.getTerm(),
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	return rf.requestVoteToPeers(ctx, req)
}

func (rf *Raft) requestVoteToPeers(ctx context.Context, req *RequestVoteArgs) <-chan *RequestVoteReply {

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
