package kvdb

import (
	"sync"
)

type CMap[K comparable, V string] struct {
	db     map[K]V
	dbLock sync.RWMutex
}

func Make[K comparable, V string]() *CMap[K, V] {
	kvdb := &CMap[K, V]{
		db:     make(map[K]V),
		dbLock: sync.RWMutex{},
	}

	return kvdb
}

func (kv *CMap[K, V]) Get(key K) (V, bool) {
	// kv.dbLock.RLock()
	// defer kv.dbLock.RUnlock()

	val, ok := kv.db[key]
	return val, ok
}

func (kv *CMap[K, V]) Put(key K, value V) {
	// kv.dbLock.Lock()
	// defer kv.dbLock.Unlock()

	kv.db[key] = value
}

func (kv *CMap[K, string]) Append(key K, value string) {
	// kv.dbLock.Lock()
	// defer kv.dbLock.Unlock()
	kv.db[key] += value
}
