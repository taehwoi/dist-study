package raft

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
