package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // Start, Get, Put, Append
	Arguments []any  // arguments
	Idx       uint64
	// TODO: maybe store also the results?
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// isLeader  bool
	idx       uint64
	observers map[uint64]chan ApplyResult
	// Key Value State
	state map[string]string
}

type ApplyResult struct {
	Value string
	Err   Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	idx := atomic.AddUint64(&kv.idx, 1)
	op := Op{Type: "Get", Arguments: []any{args.Key}, Idx: idx}

	_, l := kv.rf.GetState()
	if !l {
		// Just return value
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.observers[idx] = make(chan ApplyResult)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.observers, idx)
		kv.mu.Unlock()
	}()

	go kv.rf.Start(op)
	/*
		_, _, leader := kv.rf.Start(op)
		if !leader {
			// Just return value
			reply.Err = ErrWrongLeader
			return
		}*/

	fmt.Printf("[server] Get recieved and sent to queue: Get[%v]\n", args.Key)
	result := <-kv.observers[idx]
	// Read value
	if result.Err == ErrNoKey {
		result.Err = ErrNoKey
	} else {
		reply.Value = result.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	idx := atomic.AddUint64(&kv.idx, 1)
	var op Op
	if args.Op == "Put" {
		op = Op{Type: "Put", Arguments: []any{args.Key, args.Value}, Idx: idx}
	} else if args.Op == "Append" {
		op = Op{Type: "Append", Arguments: []any{args.Key, args.Value}, Idx: idx}
	} else {
		panic("PutAppend: unsupported op")
	}

	_, l := kv.rf.GetState()
	if !l {
		// Just return value
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.observers[idx] = make(chan ApplyResult)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.observers, idx)
		kv.mu.Unlock()
	}()

	// _, _, leader := kv.rf.Start(op)
	go kv.rf.Start(op)
	/*
		if !leader {
			// Just return value
			reply.Err = ErrWrongLeader
			return
		}*/
	fmt.Printf("[server] PutAppend recieved and sent to queue: PutAppend[%v] = %v\n", args.Key, args.Value)
	<-kv.observers[idx]
}

func (kv *KVServer) RaftApplyHandler() {
	for !kv.killed() {
		msg := <-kv.applyCh
		op, ok := msg.Command.(Op)
		if !ok {
			panic("unsupported type in Command")
		}
		fmt.Printf("[server] op recieved: %+v\n", op)
		switch op.Type {
		case "Get":
			kv.HandleGet(op)
		case "Put":
			kv.HandlePut(op)
		case "Append":
			kv.HandleAppend(op)
		default:
			fmt.Printf("[server] handle op: %v\n", op)
		}
		fmt.Printf("[server] op handled: %+v\n", op)
	}
}

func (kv *KVServer) HandleGet(op Op) {
	key, ok := op.Arguments[0].(string)
	if !ok {
		panic("cannot typecast op arguments[0]")
	}
	value, ok := kv.state[key]
	if !ok {
		kv.observers[op.Idx] <- ApplyResult{
			Err: ErrNoKey,
		}
		close(kv.observers[op.Idx])
		return
	}

	kv.observers[op.Idx] <- ApplyResult{
		Value: value,
	}
	close(kv.observers[op.Idx])
}

func (kv *KVServer) HandlePut(op Op) {
	key, ok := op.Arguments[0].(string)
	if !ok {
		panic("cannot typecast op arguments[0]")
	}
	value, ok := op.Arguments[1].(string)
	if !ok {
		panic("cannot typecast op arguments[0]")
	}

	kv.state[key] = value
	kv.observers[op.Idx] <- ApplyResult{}
	close(kv.observers[op.Idx])
}

func (kv *KVServer) HandleAppend(op Op) {
	key, ok := op.Arguments[0].(string)
	if !ok {
		panic("cannot typecast op arguments[0]")
	}
	value, ok := op.Arguments[1].(string)
	if !ok {
		panic("cannot typecast op arguments[0]")
	}

	kv.state[key] += value
	kv.observers[op.Idx] <- ApplyResult{}
	close(kv.observers[op.Idx])
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.observers = make(map[uint64]chan ApplyResult)
	kv.state = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	rf := kv.rf
	// Index, Term, Leader
	_, _, leader := rf.Start(Op{Type: "Init"})
	if leader {
		fmt.Printf("[server] %v server is leader\n", me)
	} else {
		fmt.Printf("[server] %v server is not leader\n", me)
	}
	// kv.isLeader = leader
	go func() {
		kv.RaftApplyHandler()
	}()

	return kv
}
