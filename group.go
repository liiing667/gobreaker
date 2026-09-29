package gobreaker

import (
	"container/list"
	"sync"
)

// CircuitBreakerGroup manages a set of CircuitBreakers keyed by an arbitrary
// string (e.g. a downstream service name). Each key gets its own
// CircuitBreaker with independent State and Counts, so one key tripping open
// never affects the others.
//
// If maxKeys is positive, the group keeps at most maxKeys breakers and evicts
// the least recently used key (by request traffic) when a new key would
// exceed the limit. Evicting a key discards its breaker entirely, including
// an open state and its Counts; nothing is carried over to any other key.
// If maxKeys is less than or equal to 0, the number of keys is unbounded.
//
// The given Settings is used as a template for every per-key breaker. The
// Name of each breaker is set to the key, prefixed with the template Name
// and "/" if the template Name is not empty, so OnStateChange callbacks can
// tell keys apart.
//
// The zero value is not usable; use NewCircuitBreakerGroup.
type CircuitBreakerGroup struct {
	settings Settings
	maxKeys  int

	mutex   sync.Mutex
	entries map[string]*list.Element
	lru     *list.List // front is most recently used; Value is *groupEntry
}

type groupEntry struct {
	key string
	cb  *CircuitBreaker
}

// NewCircuitBreakerGroup returns a new CircuitBreakerGroup that creates
// per-key CircuitBreakers from the given Settings template. See the
// CircuitBreakerGroup documentation for the meaning of maxKeys.
func NewCircuitBreakerGroup(st Settings, maxKeys int) *CircuitBreakerGroup {
	return &CircuitBreakerGroup{
		settings: st,
		maxKeys:  maxKeys,
		entries:  make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Execute runs the given request against the breaker for the given key,
// creating the breaker on first use. It behaves exactly like
// CircuitBreaker.Execute on the per-key breaker.
func (g *CircuitBreakerGroup) Execute(key string, req func() (interface{}, error)) (interface{}, error) {
	return g.get(key).Execute(req)
}

// Allow checks whether a new request for the given key can proceed, creating
// the breaker on first use. It behaves exactly like
// TwoStepCircuitBreaker.Allow on the per-key breaker: the returned callback
// must be used to report the outcome.
func (g *CircuitBreakerGroup) Allow(key string) (done func(success bool), err error) {
	cb := g.get(key)
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		cb.afterRequest(generation, success)
	}, nil
}

// State returns the current state of the breaker for the given key.
// It does not create a breaker; an unknown key reports StateClosed.
// Reads do not affect LRU eviction order.
func (g *CircuitBreakerGroup) State(key string) State {
	g.mutex.Lock()
	elem, ok := g.entries[key]
	g.mutex.Unlock()

	if !ok {
		return StateClosed
	}
	return elem.Value.(*groupEntry).cb.State()
}

// Counts returns a snapshot of the internal counters of the breaker for the
// given key. It does not create a breaker; an unknown key reports zero
// Counts. Reads do not affect LRU eviction order.
func (g *CircuitBreakerGroup) Counts(key string) Counts {
	g.mutex.Lock()
	elem, ok := g.entries[key]
	g.mutex.Unlock()

	if !ok {
		return Counts{}
	}
	return elem.Value.(*groupEntry).cb.Counts()
}

// Len returns the number of breakers currently held by the group.
func (g *CircuitBreakerGroup) Len() int {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return len(g.entries)
}

// Keys returns the keys currently held by the group, from most to least
// recently used.
func (g *CircuitBreakerGroup) Keys() []string {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	keys := make([]string, 0, len(g.entries))
	for e := g.lru.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*groupEntry).key)
	}
	return keys
}

// get returns the breaker for the key, creating and LRU-inserting it if
// necessary, and marks the key as most recently used.
func (g *CircuitBreakerGroup) get(key string) *CircuitBreaker {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if elem, ok := g.entries[key]; ok {
		g.lru.MoveToFront(elem)
		return elem.Value.(*groupEntry).cb
	}

	st := g.settings
	if st.Name == "" {
		st.Name = key
	} else {
		st.Name = st.Name + "/" + key
	}
	entry := &groupEntry{key: key, cb: NewCircuitBreaker(st)}
	g.entries[key] = g.lru.PushFront(entry)

	if g.maxKeys > 0 {
		for len(g.entries) > g.maxKeys {
			g.evictOldest()
		}
	}
	return entry.cb
}

// evictOldest removes the least recently used breaker. The caller must hold
// g.mutex. The evicted breaker's state and counts are discarded with it and
// never merged into any other key.
func (g *CircuitBreakerGroup) evictOldest() {
	elem := g.lru.Back()
	if elem == nil {
		return
	}
	g.lru.Remove(elem)
	delete(g.entries, elem.Value.(*groupEntry).key)
}
