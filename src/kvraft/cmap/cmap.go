package cmap

import (
	"sync"
)

type Addable interface {
	int | string
}

type CMap[K comparable, V Addable] struct {
	db     map[K]V
	dbLock sync.RWMutex
}

func Make[K comparable, V Addable]() *CMap[K, V] {
	kvdb := &CMap[K, V]{
		db:     make(map[K]V),
		dbLock: sync.RWMutex{},
	}

	return kvdb
}

func MakeFrom[K comparable, V Addable](data map[K]V) *CMap[K, V] {
	kvdb := &CMap[K, V]{
		db:     make(map[K]V),
		dbLock: sync.RWMutex{},
	}

	for k, v := range data {
		kvdb.db[k] = v
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

func (kv *CMap[K, V]) Append(key K, value V) {
	// kv.dbLock.Lock()
	// defer kv.dbLock.Unlock()
	kv.db[key] += value
}

func (kv *CMap[K, V]) Copy() map[K]V {
	res := make(map[K]V)
	// kv.dbLock.RLock()
	// defer kv.dbLock.RUnlock()

	for k, v := range kv.db {
		res[k] = v
	}

	return res
}
