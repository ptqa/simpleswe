package controller

import (
	"context"
	"sync"
)

type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	token chan struct{}
	refs  int
}

func newKeyedLocks() *keyedLocks { return &keyedLocks{locks: make(map[string]*keyedLock)} }

func (k *keyedLocks) lock(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()
	entry := k.locks[key]
	if entry == nil {
		entry = &keyedLock{token: make(chan struct{}, 1)}
		k.locks[key] = entry
	}
	entry.refs++
	k.mu.Unlock()
	select {
	case entry.token <- struct{}{}:
		return func() {
			<-entry.token
			k.release(key, entry)
		}, nil
	case <-ctx.Done():
		k.release(key, entry)
		return nil, ctx.Err()
	}
}

func (k *keyedLocks) release(key string, entry *keyedLock) {
	k.mu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(k.locks, key)
	}
	k.mu.Unlock()
}
