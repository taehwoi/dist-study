package raft

import (
	"log"

	"golang.org/x/exp/constraints"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Max[T constraints.Ordered](x, y T) T {
	if x > y {
		return x
	} else {
		return y
	}
}

func Min[T constraints.Ordered](x, y T) T {
	if x < y {
		return x
	} else {
		return y
	}
}
