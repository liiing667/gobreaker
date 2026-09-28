package gobreaker

import (
	"container/list"
	"sync"
)

// KeyedCircuitBreakers manages a set of CircuitBreakers isolated by key
// (e.g. a downstream service name). Each key gets its own CircuitBreaker
// with independent Counts and State, so one key tripping open never
// affects the others.
//
// MaxKeys bounds the number of keys held by the group. When a new key
// would exceed MaxKeys, the least recently used key is evicted. Evicting
// a key drops its CircuitBreaker entirely, so the counts of an evicted
// (even open) key are never merged into any other key. If MaxKeys is
// less than or equal to 0, the number of keys is unbounded.
//
// Each breaker is configured from the Settings given to
// NewKeyedCircuitBreakers. The breaker's name is the key itself when
// Settings.Name is empty, otherwise "Name/key".
type KeyedCircuitBreakers struct {
	settings Settings
	maxKeys  int

	mutex   sync.Mutex
	entries map[string]*list.Element
	lru     *list.List // front is most recently used
}

type keyedEntry struct {
	key string
	cb  *CircuitBreaker
}

// NewKeyedCircuitBreakers returns a new KeyedCircuitBreakers whose
// breakers are configured with the given Settings. maxKeys limits the
// number of keys kept by the group; a non-positive value means no limit.
func NewKeyedCircuitBreakers(st Settings, maxKeys int) *KeyedCircuitBreakers {
	return &KeyedCircuitBreakers{
		settings: st,
		maxKeys:  maxKeys,
		entries:  make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Breaker returns the CircuitBreaker for the given key, creating it on
// first use. The key becomes the most recently used one.
func (kcb *KeyedCircuitBreakers) Breaker(key string) *CircuitBreaker {
	kcb.mutex.Lock()
	defer kcb.mutex.Unlock()

	if elem, ok := kcb.entries[key]; ok {
		kcb.lru.MoveToFront(elem)
		return elem.Value.(*keyedEntry).cb
	}

	st := kcb.settings
	if st.Name == "" {
		st.Name = key
	} else {
		st.Name = st.Name + "/" + key
	}
	cb := NewCircuitBreaker(st)

	elem := kcb.lru.PushFront(&keyedEntry{key: key, cb: cb})
	kcb.entries[key] = elem
	kcb.evictIfNeeded()

	return cb
}

// Execute runs the given request through the CircuitBreaker of the given
// key. It behaves exactly like CircuitBreaker.Execute.
func (kcb *KeyedCircuitBreakers) Execute(key string, req func() (interface{}, error)) (interface{}, error) {
	return kcb.Breaker(key).Execute(req)
}

// State returns the current state of the given key. The second return
// value reports whether the key is currently held by the group.
func (kcb *KeyedCircuitBreakers) State(key string) (State, bool) {
	cb, ok := kcb.peek(key)
	if !ok {
		return StateClosed, false
	}
	return cb.State(), true
}

// Counts returns a snapshot of the internal counters of the given key
// for the current window. The second return value reports whether the
// key is currently held by the group.
func (kcb *KeyedCircuitBreakers) Counts(key string) (Counts, bool) {
	cb, ok := kcb.peek(key)
	if !ok {
		return Counts{}, false
	}
	return cb.Counts(), true
}

// Len returns the number of keys currently held by the group.
func (kcb *KeyedCircuitBreakers) Len() int {
	kcb.mutex.Lock()
	defer kcb.mutex.Unlock()

	return len(kcb.entries)
}

// peek returns the CircuitBreaker for key without marking it as used.
func (kcb *KeyedCircuitBreakers) peek(key string) (*CircuitBreaker, bool) {
	kcb.mutex.Lock()
	defer kcb.mutex.Unlock()

	elem, ok := kcb.entries[key]
	if !ok {
		return nil, false
	}
	return elem.Value.(*keyedEntry).cb, true
}

// evictIfNeeded removes least recently used keys until the group fits
// within maxKeys. Callers must hold kcb.mutex.
func (kcb *KeyedCircuitBreakers) evictIfNeeded() {
	if kcb.maxKeys <= 0 {
		return
	}
	for len(kcb.entries) > kcb.maxKeys {
		elem := kcb.lru.Back()
		if elem == nil {
			return
		}
		entry := elem.Value.(*keyedEntry)
		delete(kcb.entries, entry.key)
		kcb.lru.Remove(elem)
	}
}
