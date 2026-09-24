package ctxutil

import "context"

// OnCancel runs cancel once ctx ends. The returned stop prevents a pending
// call and, if cancel has already started, waits for it to finish. Callers use
// it to abort blocking I/O on a resource and then hand that resource to a new
// owner, knowing cancel can no longer touch it.
func OnCancel(ctx context.Context, cancel func()) (stop func()) {
	done := make(chan struct{})
	stopAfterFunc := context.AfterFunc(ctx, func() {
		defer close(done)
		cancel()
	})
	return func() {
		if !stopAfterFunc() {
			<-done
		}
	}
}
