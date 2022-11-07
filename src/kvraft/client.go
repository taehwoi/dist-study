package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const callRetryCount = 100

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIdx uint32
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
	// You'll have to add code here.

	// We need to know which one is leader
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
	// You will have to modify this function.
	tryServerGet := func() (string, error) {
		args := GetArgs{
			Key: key,
		}
		res := GetReply{}
		ok := ck.Leader().Call("KVServer.Get", &args, &res)
		if !ok {
			// Try again, with a new server
			return "", fmt.Errorf("KVServer.Get Call timeout")
		}

		if res.Err != "" {
			return "", fmt.Errorf("KVServer.Get returns error: %v", res.Err)
		}
		return res.Value, nil
	}

	for i := 0; i < callRetryCount; i++ {
		res, err := tryServerGet()
		if err != nil {
			fmt.Printf("[client] server (%v) get: %v\n", ck.leaderIdx, err)
			ck.GuessNextLeader()
			time.Sleep(time.Millisecond * 50)
		} else {
			fmt.Printf("[client] server (%v) get[%v]: %v\n", ck.leaderIdx, key, res)
			return res
		}
	}
	return ""
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
	tryServerPutAppend := func() error {
		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
		}
		res := PutAppendReply{}
		ok := ck.Leader().Call("KVServer.PutAppend", &args, &res)
		if !ok {
			// Try again, with a new server
			return fmt.Errorf("KVServer.PutAppend Call timeout")
		}

		if res.Err != "" {
			return fmt.Errorf("KVServer.PutAppend returns error: %v", res.Err)
		}
		return nil
	}

	for i := 0; i < callRetryCount; i++ {
		err := tryServerPutAppend()
		if err != nil {
			fmt.Printf("[client] server (%v) putAppend: %v\n", ck.leaderIdx, err)
			ck.GuessNextLeader()
			time.Sleep(time.Millisecond * 50)
		} else {
			fmt.Printf("[client] server (%v) putAppend[%v]: %v\n", ck.leaderIdx, key, value)
			return
		}
	}

	fmt.Printf("server put append retry failed")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// Helper functions
func (ck *Clerk) Leader() *labrpc.ClientEnd {
	return ck.servers[ck.leaderIdx]
}

// If the connected client is not a leader, try to guess a new leader
func (ck *Clerk) GuessNextLeader() {
	// ck.leaderIdx = (ck.leaderIdx + 1) % uint(len(ck.servers))
	done := false
	newIdx := uint32(nrand() % int64(len(ck.servers)))
	for !done {
		leaderIdx := ck.leaderIdx
		done = atomic.CompareAndSwapUint32(&ck.leaderIdx, leaderIdx, newIdx)
	}
}
