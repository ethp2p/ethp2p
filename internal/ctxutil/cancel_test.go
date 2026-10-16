package ctxutil

import (
	"context"
	"testing"
	"time"
)

func TestOnCancel(t *testing.T) {
	t.Run("stopped before cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		called := make(chan struct{})
		stop := OnCancel(ctx, func() { close(called) })
		stop()
		cancel()
		stop()
		select {
		case <-called:
			t.Fatal("callback ran after stop")
		default:
		}
	})

	t.Run("joins running callback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		started := make(chan struct{})
		finish := make(chan struct{})
		stopped := make(chan struct{})
		stop := OnCancel(ctx, func() {
			close(started)
			<-finish
		})
		cancel()
		<-started
		go func() {
			stop()
			close(stopped)
		}()
		select {
		case <-stopped:
			t.Fatal("stop returned before callback finished")
		case <-time.After(10 * time.Millisecond):
		}
		close(finish)
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("stop did not return after callback finished")
		}
		stop()
	})
}
