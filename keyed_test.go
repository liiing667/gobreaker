package gobreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestKeyedCB(maxKeys int) *KeyedCircuitBreakers {
	return NewKeyedCircuitBreakers(Settings{
		MaxRequests: 1,
		Interval:    time.Duration(0),
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
	}, maxKeys)
}

func keyedFail(kcb *KeyedCircuitBreakers, key string) error {
	_, err := kcb.Execute(key, func() (interface{}, error) {
		return nil, errors.New("fail")
	})
	if err != nil && err.Error() == "fail" {
		return nil
	}
	return err
}

func TestKeyedIsolation(t *testing.T) {
	kcb := newTestKeyedCB(0)

	assert.NoError(t, keyedFail(kcb, "svc-a"))

	state, ok := kcb.State("svc-a")
	assert.True(t, ok)
	assert.Equal(t, StateOpen, state)

	// svc-a is open, but svc-b must stay closed and functional.
	state, ok = kcb.State("svc-b")
	assert.False(t, ok)
	assert.Equal(t, StateClosed, state)

	_, err := kcb.Execute("svc-b", func() (interface{}, error) { return "ok", nil })
	assert.NoError(t, err)

	state, ok = kcb.State("svc-b")
	assert.True(t, ok)
	assert.Equal(t, StateClosed, state)

	_, err = kcb.Execute("svc-a", func() (interface{}, error) { return nil, nil })
	assert.Equal(t, ErrOpenState, err)
}

func TestKeyedCountsSnapshot(t *testing.T) {
	kcb := newTestKeyedCB(0)

	_, err := kcb.Execute("svc-a", func() (interface{}, error) { return nil, nil })
	assert.NoError(t, err)
	_, err = kcb.Execute("svc-a", func() (interface{}, error) { return nil, nil })
	assert.NoError(t, err)

	counts, ok := kcb.Counts("svc-a")
	assert.True(t, ok)
	assert.Equal(t, uint32(2), counts.Requests)
	assert.Equal(t, uint32(2), counts.TotalSuccesses)

	counts, ok = kcb.Counts("svc-missing")
	assert.False(t, ok)
	assert.Equal(t, Counts{}, counts)
}

func TestKeyedLRUeviction(t *testing.T) {
	kcb := newTestKeyedCB(2)

	kcb.Breaker("svc-a")
	kcb.Breaker("svc-b")
	assert.Equal(t, 2, kcb.Len())

	// Touch svc-a so svc-b becomes the least recently used.
	kcb.Breaker("svc-a")

	kcb.Breaker("svc-c")
	assert.Equal(t, 2, kcb.Len())

	_, ok := kcb.State("svc-b")
	assert.False(t, ok, "svc-b should have been evicted")
	_, ok = kcb.State("svc-a")
	assert.True(t, ok)
	_, ok = kcb.State("svc-c")
	assert.True(t, ok)
}

func TestKeyedEvictOpenKeyKeepsOthersClean(t *testing.T) {
	kcb := newTestKeyedCB(3)

	// Trip svc-a open and record some traffic on svc-b.
	assert.NoError(t, keyedFail(kcb, "svc-a"))
	_, err := kcb.Execute("svc-b", func() (interface{}, error) { return nil, nil })
	assert.NoError(t, err)

	// Evict the open svc-a by adding two more keys.
	kcb.Breaker("svc-c")
	kcb.Breaker("svc-d")
	_, ok := kcb.State("svc-a")
	assert.False(t, ok, "svc-a should have been evicted")

	// svc-b's counts must not include svc-a's failure.
	counts, ok := kcb.Counts("svc-b")
	assert.True(t, ok)
	assert.Equal(t, Counts{
		Requests:             1,
		TotalSuccesses:       1,
		ConsecutiveSuccesses: 1,
	}, counts)

	// A re-created svc-a starts from a fresh, closed breaker.
	kcb.Breaker("svc-a")
	state, ok := kcb.State("svc-a")
	assert.True(t, ok)
	assert.Equal(t, StateClosed, state)
	counts, ok = kcb.Counts("svc-a")
	assert.True(t, ok)
	assert.Equal(t, Counts{}, counts)
}

func TestKeyedHalfOpenMaxRequests(t *testing.T) {
	kcb := newTestKeyedCB(0)

	assert.NoError(t, keyedFail(kcb, "svc-a"))
	state, ok := kcb.State("svc-a")
	assert.True(t, ok)
	assert.Equal(t, StateOpen, state)

	// Move svc-a to half-open.
	pseudoSleep(kcb.Breaker("svc-a"), 10*time.Second)
	state, ok = kcb.State("svc-a")
	assert.True(t, ok)
	assert.Equal(t, StateHalfOpen, state)

	// Occupy the single half-open probe slot.
	blocked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := kcb.Execute("svc-a", func() (interface{}, error) {
			close(blocked)
			<-release
			return nil, nil
		})
		assert.NoError(t, err)
	}()
	<-blocked

	_, err := kcb.Execute("svc-a", func() (interface{}, error) { return nil, nil })
	assert.Equal(t, ErrTooManyRequests, err)

	// Other keys are not affected by svc-a's half-open limit.
	_, err = kcb.Execute("svc-b", func() (interface{}, error) { return nil, nil })
	assert.NoError(t, err)

	close(release)
	<-done

	state, ok = kcb.State("svc-a")
	assert.True(t, ok)
	assert.Equal(t, StateClosed, state)
}

func TestKeyedBreakerName(t *testing.T) {
	kcb := NewKeyedCircuitBreakers(Settings{}, 0)
	assert.Equal(t, "svc-a", kcb.Breaker("svc-a").Name())

	named := NewKeyedCircuitBreakers(Settings{Name: "order"}, 0)
	assert.Equal(t, "order/svc-a", named.Breaker("svc-a").Name())
}

func TestKeyedUnlimitedKeys(t *testing.T) {
	kcb := newTestKeyedCB(0)
	for _, key := range []string{"a", "b", "c", "d"} {
		kcb.Breaker(key)
	}
	assert.Equal(t, 4, kcb.Len())
}
