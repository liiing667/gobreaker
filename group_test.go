package gobreaker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestGroup(maxKeys int) *CircuitBreakerGroup {
	var st Settings
	st.MaxRequests = 1
	st.Timeout = 60 * time.Second
	st.ReadyToTrip = func(counts Counts) bool {
		return counts.ConsecutiveFailures >= 2
	}
	return NewCircuitBreakerGroup(st, maxKeys)
}

func groupFail(g *CircuitBreakerGroup, key string) error {
	_, err := g.Execute(key, func() (interface{}, error) {
		return nil, errors.New("fail")
	})
	if err != nil && err.Error() == "fail" {
		return nil
	}
	return err
}

func groupSucceed(g *CircuitBreakerGroup, key string) error {
	_, err := g.Execute(key, func() (interface{}, error) {
		return nil, nil
	})
	return err
}

func TestGroupKeyIsolation(t *testing.T) {
	g := newTestGroup(0)

	assert.NoError(t, groupFail(g, "svc-a"))
	assert.Equal(t, uint32(1), g.Counts("svc-a").TotalFailures)
	assert.NoError(t, groupFail(g, "svc-a"))
	assert.Equal(t, StateOpen, g.State("svc-a"))
	assert.Equal(t, ErrOpenState, groupFail(g, "svc-a"))

	// svc-b is unaffected by svc-a tripping open.
	assert.Equal(t, StateClosed, g.State("svc-b"))
	assert.NoError(t, groupSucceed(g, "svc-b"))
	assert.Equal(t, Counts{Requests: 1, TotalSuccesses: 1, ConsecutiveSuccesses: 1}, g.Counts("svc-b"))

	// svc-a's counts were cleared on the transition to open, per Counts
	// semantics, and were never mixed into svc-b.
	assert.Equal(t, Counts{}, g.Counts("svc-a"))
}

func TestGroupLRUEviction(t *testing.T) {
	g := newTestGroup(2)

	assert.NoError(t, groupSucceed(g, "a"))
	assert.NoError(t, groupSucceed(g, "b"))
	assert.NoError(t, groupSucceed(g, "a")) // a is now most recently used
	assert.NoError(t, groupSucceed(g, "c")) // evicts b

	assert.Equal(t, 2, g.Len())
	assert.Equal(t, []string{"c", "a"}, g.Keys())

	// b was evicted: its counts are gone and it reports as a fresh key.
	assert.Equal(t, Counts{}, g.Counts("b"))
	assert.Equal(t, StateClosed, g.State("b"))

	// a kept its counts across the eviction.
	assert.Equal(t, uint32(2), g.Counts("a").TotalSuccesses)
}

func TestGroupEvictOpenKeyDoesNotLeak(t *testing.T) {
	g := newTestGroup(1)

	assert.NoError(t, groupFail(g, "a"))
	assert.NoError(t, groupFail(g, "a"))
	assert.Equal(t, StateOpen, g.State("a"))

	// Creating "b" evicts the open "a"; b must start from clean counts.
	assert.NoError(t, groupSucceed(g, "b"))
	assert.Equal(t, StateClosed, g.State("b"))
	assert.Equal(t, Counts{Requests: 1, TotalSuccesses: 1, ConsecutiveSuccesses: 1}, g.Counts("b"))

	// Recreating "a" starts fresh instead of resurrecting the open state.
	assert.NoError(t, groupSucceed(g, "a"))
	assert.Equal(t, StateClosed, g.State("a"))
	assert.Equal(t, Counts{Requests: 1, TotalSuccesses: 1, ConsecutiveSuccesses: 1}, g.Counts("a"))
}

func TestGroupHalfOpenMaxRequests(t *testing.T) {
	g := newTestGroup(0)

	assert.NoError(t, groupFail(g, "a"))
	assert.NoError(t, groupFail(g, "a"))
	assert.Equal(t, StateOpen, g.State("a"))

	// Expire the open-state timeout of the per-key breaker.
	g.mutex.Lock()
	cb := g.entries["a"].Value.(*groupEntry).cb
	g.mutex.Unlock()
	pseudoSleep(cb, 61*time.Second)

	done, err := g.Allow("a")
	assert.NoError(t, err)
	assert.Equal(t, StateHalfOpen, g.State("a"))

	// MaxRequests is 1, so the next probe is rejected.
	_, err = g.Allow("a")
	assert.Equal(t, ErrTooManyRequests, err)

	done(true)
	assert.Equal(t, StateClosed, g.State("a"))
}

func TestGroupStateAndCountsSnapshot(t *testing.T) {
	g := newTestGroup(0)

	// Reads do not create entries.
	assert.Equal(t, StateClosed, g.State("missing"))
	assert.Equal(t, Counts{}, g.Counts("missing"))
	assert.Equal(t, 0, g.Len())

	assert.NoError(t, groupSucceed(g, "a"))
	assert.NoError(t, groupFail(g, "a"))
	counts := g.Counts("a")
	assert.Equal(t, uint32(2), counts.Requests)
	assert.Equal(t, uint32(1), counts.TotalSuccesses)
	assert.Equal(t, uint32(1), counts.TotalFailures)
	assert.Equal(t, uint32(1), counts.ConsecutiveFailures)
}

func TestGroupOnStateChangeNamesKey(t *testing.T) {
	var mu sync.Mutex
	changes := make(map[string]State)

	var st Settings
	st.Name = "order"
	st.ReadyToTrip = func(counts Counts) bool { return counts.ConsecutiveFailures >= 1 }
	st.OnStateChange = func(name string, from State, to State) {
		mu.Lock()
		changes[name] = to
		mu.Unlock()
	}
	g := NewCircuitBreakerGroup(st, 0)

	assert.NoError(t, groupFail(g, "payment"))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, StateOpen, changes["order/payment"])
}

func TestGroupUnlimitedKeys(t *testing.T) {
	g := newTestGroup(0)
	for _, key := range []string{"a", "b", "c", "d"} {
		assert.NoError(t, groupSucceed(g, key))
	}
	assert.Equal(t, 4, g.Len())
}

func TestGroupConcurrent(t *testing.T) {
	g := newTestGroup(4)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		for _, key := range []string{"a", "b", "c", "d", "e"} {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				_ = groupSucceed(g, key)
				_ = g.State(key)
				_ = g.Counts(key)
			}(key)
		}
	}
	wg.Wait()

	assert.Equal(t, 4, g.Len())
}
