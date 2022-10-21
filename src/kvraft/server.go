package kvraft

import (
	"context"
	"fmt"

	"6.824/kvraft/kvdb"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		// log.Printf(format, a...)
	}
	return
}

type operation string

const (
	GET    operation = "Get"
	PUT    operation = "Put"
	APPEND operation = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  operation
	Key   string
	Value string
	UID   int64
	CID   int64
}

type Reply struct {
	err Err
	val string

	cid int64 // cid of request
	uid int64 // uid of request
}

type OpFuture struct {
	op      Op
	replyCh chan *Reply
}

type KVServer struct {
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	commandCh chan *OpFuture

	db *kvdb.CMap[string, string]

	maxraftstate int // snapshot if log grows this big

	cancelFunc context.CancelFunc

	futureCh chan *OpFuture
	doneCh   chan *Reply
}

func (kv *KVServer) run(ctx context.Context) {
	// SHOULD run reciever and applier in seprate goroutines
	// if we don't a deadlock can happen with the raft Server; if raft server applies and KVServer submits command,
	// KVServer can't read the applied msg and raft server can't read the submitted command

	go kv.runCmdReciever(ctx)
	go kv.runCmdReplier(ctx)
	go kv.runCmdApplier(ctx)
}

func (kv *KVServer) runCmdReciever(ctx context.Context) {
	for {
		select {
		case opFuture := <-kv.commandCh:
			op := opFuture.op

			// store future, so it can be later replied
			kv.futureCh <- opFuture

			_, _, isLeader := kv.rf.Start(op)

			if !isLeader {
				fmt.Printf("%d %v went to wrong leader \n", kv.me, op)
				kv.doneCh <- &Reply{err: ErrWrongLeader, uid: op.UID, cid: op.CID}
			} else {
				fmt.Printf("%d %v went to leader \n", kv.me, op)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (kv *KVServer) runCmdApplier(ctx context.Context) {
	fmt.Println("running the main loop...")

	lastClientReply := make(map[int64]*Reply)

	for {
		select {
		// NOTE: blocking on applyCh creates a deadlock raft; so read asap
		case res := <-kv.applyCh:
			// apply command to db, as it has been committed to raft
			op := res.Command.(Op)
			res.CommandIndex

			fmt.Printf("%d raft has agreed on %v\n", kv.me, op)
			var reply *Reply

			if cachedReply, ok := lastClientReply[op.CID]; ok {
				// already replied
				if cachedReply.uid == op.UID {
					fmt.Printf("duplicate request---------------------!!!!!!!!!!!!!!!!!!!!!!!\n")
					kv.doneCh <- cachedReply
					// no need to apply to db
					break
				}
			}
			switch op.Type {
			case GET:
				v, ok := kv.db.Get(op.Key)

				if !ok {
					reply = &Reply{val: "", err: ErrNoKey, uid: op.UID, cid: op.CID}
				} else {
					reply = &Reply{val: v, err: OK, uid: op.UID, cid: op.CID}
				}
			case PUT:
				kv.db.Put(op.Key, op.Value)

				reply = &Reply{err: OK, val: op.Value, uid: op.UID, cid: op.CID}
			case APPEND:
				kv.db.Append(op.Key, op.Value)

				reply = &Reply{err: OK, val: op.Value, uid: op.UID, cid: op.CID}
			default:
				fmt.Println("should not get here!!!!!!!!!!!!!!!!!!!!!!!")
			}

			// store the cmd, so it is not executed again by client retry
			if reply.err == OK {
				lastClientReply[reply.cid] = reply
				// fmt.Printf("cached %v \n", lastClientReply)
			}

			kv.doneCh <- reply

		case <-ctx.Done():
			return
		}
	}
}

func (kv *KVServer) runCmdReplier(ctx context.Context) {
	// holds channels to reply stuff
	chMap := make(map[int64]chan *Reply)

	for {
		select {
		case future := <-kv.futureCh:
			fmt.Printf("-------------------recieved future----\n")

			chMap[future.op.UID] = future.replyCh

		case reply := <-kv.doneCh:
			fmt.Printf("-------------------recieved done----\n")
			uid := reply.uid
			if ch, ok := chMap[uid]; ok {
				if ch != nil {
					go func() {
						defer close(ch)
						ch <- reply
					}()
				}
				delete(chMap, uid)
			}

		case <-ctx.Done():
			return
		}
	}

}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	fmt.Printf("recieved get %v\n", args)
	op := Op{Type: GET, Key: args.Key, UID: args.UID, CID: args.CID}
	replyCh := make(chan *Reply)

	fmt.Printf("try submitting get %v\n", args)
	kv.commandCh <- &OpFuture{op: op, replyCh: replyCh}
	fmt.Printf("submitted get %v\n", args)

	// wait until applied
	res := <-replyCh
	reply.Err = res.err
	reply.Value = res.val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	fmt.Printf("recieved putAppend %v\n", args)
	var op Op
	if args.Op == "Put" {
		op = Op{Type: PUT, Key: args.Key, Value: args.Value, UID: args.UID, CID: args.CID}
	} else if args.Op == "Append" {
		op = Op{Type: APPEND, Key: args.Key, Value: args.Value, UID: args.UID, CID: args.CID}
	}
	replyCh := make(chan *Reply)

	fmt.Printf("try submitting putAppend %v\n", args)
	kv.commandCh <- &OpFuture{op: op, replyCh: replyCh}
	fmt.Printf("submitted putAppend %v\n", args)

	// wait until applied
	res := <-replyCh
	reply.Err = res.err
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
	kv.rf.Kill()
	kv.cancelFunc()
	// Your code here, if desired.
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

	fmt.Printf("start kv server\n")
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.commandCh = make(chan *OpFuture)

	kv.futureCh = make(chan *OpFuture)
	kv.doneCh = make(chan *Reply)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = kvdb.Make[string]()

	ctx, cancel := context.WithCancel(context.Background())
	kv.cancelFunc = cancel

	kv.run(ctx)

	return kv
}
