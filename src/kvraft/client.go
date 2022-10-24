package kvraft

import (
	"crypto/rand"
	"math/big"

	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastKnownLeader int32
	cid             int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	// fmt.Printf("creating client %d\n", ck.cid)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	lastKnownLeader := atomic.LoadInt32(&ck.lastKnownLeader)
	args := GetArgs{Key: key, UID: nrand(), CID: ck.cid}
	for i := lastKnownLeader; ; i %= int32(len(ck.servers)) {
		reply := GetReply{}
		server := ck.servers[i]

		// fmt.Printf("sending %v to %d\n", args, i)
		if ok := server.Call("KVServer.Get", &args, &reply); ok {
			if reply.Err != ErrWrongLeader {
				// fmt.Printf("--------------------returning from GET---------\n")
				// fmt.Printf("------------value: %v\n", reply.Value)
				atomic.StoreInt32(&ck.lastKnownLeader, i)
				return reply.Value
			}
		}
		i++
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	lastKnownLeader := atomic.LoadInt32(&ck.lastKnownLeader)
	args := PutAppendArgs{Key: key, Value: value, Op: op, UID: nrand(), CID: ck.cid}
	for i := lastKnownLeader; ; i %= int32(len(ck.servers)) {
		reply := PutAppendReply{}
		server := ck.servers[i]
		// fmt.Printf("sending %v to %d\n", args, i)

		if ok := server.Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err != ErrWrongLeader {
				// fmt.Printf("--------------------returning from putappend---------\n")
				atomic.StoreInt32(&ck.lastKnownLeader, i)
				return
			}
		}
		i++
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
