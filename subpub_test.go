package cmd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublishSubscribe(t *testing.T) {
	t.Parallel()

	sp := NewSubPub()
	defer func(sp SubPub, ctx context.Context) {
		_ = sp.Close(ctx)
	}(sp, t.Context())

	var mu sync.Mutex
	received := make(map[string][]interface{})

	handler := func(id string) MessageHandler {
		return func(msg interface{}) {
			mu.Lock()
			defer mu.Unlock()
			received[id] = append(received[id], msg)
		}
	}

	_ = sp.Subscribe("subject", handler("first"))
	sub2 := sp.Subscribe("subject", handler("second"))

	for i := 0; i < 3; i++ {
		err := sp.Publish("subject", i)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	for _, id := range []string{"first", "second"} {
		require.Equal(t, len(received[id]), 3)
	}
	mu.Unlock()

	for _, id := range []string{"first", "second"} {
		for i, v := range received[id] {
			require.Equal(t, v.(int), i)
		}
	}

	sub2.Unsubscribe()
	err := sp.Publish("subject", "last")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, len(received["first"]), 4)
	require.Equal(t, len(received["second"]), 3)
}

func TestSlowSubscriber(t *testing.T) {
	t.Parallel()

	sp := NewSubPub()
	defer func(sp SubPub, ctx context.Context) {
		_ = sp.Close(ctx)
	}(sp, t.Context())

	fast := make(chan struct{})
	slowDone := make(chan struct{})

	sp.Subscribe("subject", func(msg interface{}) {
		fast <- struct{}{}
	})
	sp.Subscribe("subject", func(msg interface{}) {
		time.Sleep(200 * time.Millisecond)
		slowDone <- struct{}{}
	})

	start := time.Now()
	err := sp.Publish("subject", 1)
	require.NoError(t, err)

	select {
	case <-fast:
	case <-time.After(50 * time.Millisecond):
		t.Errorf("fast subscriber blocked by slow")
	}

	select {
	case <-slowDone:
	case <-time.After(300 * time.Millisecond):
		t.Errorf("slow subscriber did not finish in time")
	}

	require.Greater(t, time.Since(start), 200*time.Millisecond)
}

func TestCloseContextCancel(t *testing.T) {
	t.Parallel()

	sp := NewSubPub()

	message := func(msg interface{}) {}
	sub := sp.Subscribe("subject", message)

	for i := 0; i < 1000; i++ {
		err := sp.Publish("subject", i)
		if err != nil {
			return
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	startTime := time.Now()
	err := sp.Close(ctx)
	endTime := time.Since(startTime)

	require.Error(t, err)
	require.Greater(t, 100*time.Millisecond, endTime)

	sub.Unsubscribe()
}
