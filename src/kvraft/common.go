package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// id of request
	UID int64
	// id of client
	CID int64
}

func (s PutAppendArgs) String() string {
	return fmt.Sprintf(
		"Args{Op: %s, Key: %s, Value: '%s', UID: %d}",
		s.Op, s.Key, s.Value, s.UID)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// id of request
	UID int64
	// id of client
	CID int64
}

func (s GetArgs) String() string {
	return fmt.Sprintf(
		"Args{Key: %s, UID: %d}",
		s.Key, s.UID)
}

type GetReply struct {
	Err   Err
	Value string
}
