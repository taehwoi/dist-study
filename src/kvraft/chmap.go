package kvraft

import (
	"sync"
)

type ChMap struct {
	db     map[int64]chan *Reply
	dbLock sync.RWMutex
}

func MakeChMap() *ChMap {
	kvdb := &ChMap{
		db:     make(map[int64]chan *Reply),
		dbLock: sync.RWMutex{},
	}

	return kvdb
}

func (kv *ChMap) Get(key int64) (chan *Reply, bool) {
	// kv.dbLock.RLock()
	// defer kv.dbLock.RUnlock()

	val, ok := kv.db[key]
	return val, ok
}

func (kv *ChMap) Put(key int64, ch chan *Reply) {
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	kv.db[key] = ch
}

func (kv *ChMap) Remove(key int64) {
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	delete(kv.db, key)
}
