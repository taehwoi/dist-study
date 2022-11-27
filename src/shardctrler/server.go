package shardctrler

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	cmdCh   chan *OpFuture
	doneCh  chan *Reply
}

type operation string

const (
	JOIN  operation = "Join"
	LEAVE operation = "Leave"
	MOVE  operation = "Move"
	QUERY operation = "Query"
)

type Op struct {
	Cmd operation
	// Your data here.
	Args interface{}
}

type OpFuture struct {
	op      Op
	replyCh chan *Reply

	indexIfCommitted int
	termIfCommitted  int

	// this is included in op.Args, but putting it here makes access easy
	UID int64
}

type Reply struct {
	WrongLeader bool
	Err         Err
	Config      Config

	Cid int64 // cid of request
	UID int64 // uid of request
	Idx int   // index of committed fmt
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	fmt.Println("join")
	op := Op{
		Cmd:  JOIN,
		Args: *args,
	}
	replyCh := make(chan *Reply)
	defer close(replyCh)

	sc.cmdCh <- &OpFuture{op: op, replyCh: replyCh, UID: args.UID}

	// wait until applied
	res := <-replyCh

	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Cmd:  LEAVE,
		Args: *args,
	}
	replyCh := make(chan *Reply)
	defer close(replyCh)

	sc.cmdCh <- &OpFuture{op: op, replyCh: replyCh, UID: args.UID}

	res := <-replyCh

	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Cmd:  MOVE,
		Args: *args,
	}
	replyCh := make(chan *Reply)
	defer close(replyCh)

	sc.cmdCh <- &OpFuture{op: op, replyCh: replyCh, UID: args.UID}

	res := <-replyCh

	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Cmd:  QUERY,
		Args: *args,
	}
	replyCh := make(chan *Reply)
	defer close(replyCh)

	sc.cmdCh <- &OpFuture{op: op, replyCh: replyCh}

	res := <-replyCh

	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
	reply.Config = res.Config
}

// main thread, takes care of configs
func (sc *ShardCtrler) run(ctx context.Context) {
	go sc.runCmdReciever(ctx)
	go sc.runCmdApplier(ctx)
}

func (sc *ShardCtrler) runCmdReciever(ctx context.Context) {
	chMap := make(map[int64]chan *Reply)

	for {
		select {
		// received command from rpcs
		case opFuture := <-sc.cmdCh:
			op := opFuture.op
			UID := opFuture.UID

			// fmt.Printf("start command %v\n", opFuture.op)
			idx, term, isLeader := sc.rf.Start(op)
			// fmt.Printf("done command %v\n", opFuture.op)
			opFuture.indexIfCommitted = idx
			opFuture.termIfCommitted = term

			// store future, so it can be later replied
			// fmt.Println("store future")
			// sc.futureCh <- opFuture
			// fmt.Println("stored future")

			if !isLeader {
				// fmt.Printf("%d %v went to wrong leader \n", kv.me, op)
				// fmt.Printf("send to cmd doneCh\n")
				opFuture.replyCh <- &Reply{
					WrongLeader: true,
				}
			} else {
				chMap[UID] = opFuture.replyCh
			}

		case reply := <-sc.doneCh:
			UID := reply.UID
			if replyCh, ok := chMap[UID]; ok {
				replyCh <- reply
				delete(chMap, UID)
			}
			// TODO
			// reply uncomitted, so that clients can try again

		case <-ctx.Done():
			return
		}
		// fmt.Println("recv exited")
	}
}

// deals with sc.Configs
func (sc *ShardCtrler) runCmdApplier(ctx context.Context) {
	for {
		select {
		case res := <-sc.applyCh:

			op := res.Command.(Op)

			switch op.Cmd {
			case JOIN:
				fmt.Println("agreed on join")
				args := op.Args.(JoinArgs)
				sc.handleJoin(args)
				fmt.Println(sc.configs)

				// case MOVE:
				// args := op.Args.(MoveArgs)
				// case LEAVE:
				// args := op.Args.(LeaveArgs)
				// case QUERY:
				// args := op.Args.(QueryArgs)
			}

			sc.doneCh <- &Reply{}

		case <-ctx.Done():
			return
		}
	}

}

// should only be called in the main thread
func (sc *ShardCtrler) handleJoin(args JoinArgs) {

	latestConfig := sc.configs[len(sc.configs)-1]

	groups := make(map[int][]string)

	// copy original groups
	for k, v := range latestConfig.Groups {
		groups[k] = v
	}
	// add new groups
	for k, v := range args.Servers {
		groups[k] = append(groups[k], v...)
	}

	gids := make([]int, 0, len(groups))
	for k := range groups {
		gids = append(gids, k)
	}

	newShards := rebalance(latestConfig.Shards, gids)

	config := Config{
		Num:    len(sc.configs),
		Shards: newShards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, config)

}

// given current shard config and new list of groups, return new allocation
func rebalance(shards [NShards]int, newGroupIds []int) [NShards]int {
	res := [NShards]int{}

	currAllocation := make(map[int][]int) // gid -> shards

	currGroupIds := make([]int, 0)
	for idx, v := range shards {
		currAllocation[v] = append(currAllocation[v], idx)
		currGroupIds = append(currGroupIds, v)
	}

	newAllocationCnt := make(map[int]int) // gid -> N of shards

	// sort for determinism
	sort.Ints(newGroupIds)
	sort.Ints(currGroupIds)

	// each group should have at least q shards allocated
	q := NShards / len(newGroupIds)

	// remainder, if possible, should be allocated to group's that are already in previous config
	// this allows minimal movement of shards
	r := NShards % len(newGroupIds)

	for _, gid := range newGroupIds {
		newAllocationCnt[gid] = q
	}

	for _, gid := range currGroupIds {
		// if previous group is also in new Group, += 1 shard
		if r > 0 && newAllocationCnt[gid] > 0 {
			newAllocationCnt[gid]++
			r--
		}
	}
	for _, gid := range newGroupIds {
		// allocate remainders; lower gids are allocated more shards
		if r > 0 && newAllocationCnt[gid] == q {
			newAllocationCnt[gid]++
			r--
		}
	}

	newAllocation := make(map[int][]int)

	// shards that belong to removed group, or shards that is full
	shardsToMove := make([]int, 0)
	for gid, shards := range currAllocation {
		cnt, ok := newAllocationCnt[gid]

		// if group has been removed
		if !ok {
			shardsToMove = append(shardsToMove, shards...)
			continue
		}
		// if current group has more shards than it should
		if len(shards) > cnt {
			shardsToMove = append(shardsToMove, shards[cnt:]...)
			newAllocation[gid] = shards[:cnt]
		} else {
			newAllocation[gid] = shards
		}
	}
	// sort for determinism
	sort.Ints(shardsToMove)

	for _, v := range newGroupIds {
		var toTake []int
		n := newAllocationCnt[v] - len(newAllocation[v])
		// take first n shards from shardsToMove
		toTake, shardsToMove = shardsToMove[:n], shardsToMove[n:]
		newAllocation[v] = append(newAllocation[v], toTake...)
	}

	// newAllocation to Shards
	for gid, shards := range newAllocation {
		// fmt.Println(gid)
		for _, shard := range shards {
			// fmt.Println(shard)
			res[shard] = gid
		}
	}

	// do stuff to res

	return res
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	// initial empty config
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.doneCh = make(chan *Reply)
	sc.cmdCh = make(chan *OpFuture)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	go sc.run(context.TODO())

	return sc
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
