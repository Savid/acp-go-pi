package pi

import (
	"errors"
	"testing"
	"time"
)

func TestDirectChildWaitAwaitReaped(t *testing.T) {
	if err := (*directChildWait)(nil).awaitReaped(time.Second); err == nil {
		t.Fatal("nil direct-child waiter succeeded")
	}

	pending := &directChildWait{done: make(chan struct{})}
	if err := pending.awaitReaped(time.Nanosecond); err == nil {
		t.Fatal("pending direct-child waiter succeeded")
	}

	complete := &directChildWait{done: make(chan struct{})}
	close(complete.done)
	if err := complete.awaitReaped(time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestDirectChildWaitAwait(t *testing.T) {
	if err := (*directChildWait)(nil).await(time.Second); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("nil wait = %v", err)
	}
	pending := &directChildWait{done: make(chan struct{})}
	if err := pending.await(time.Nanosecond); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("pending wait = %v", err)
	}
	want := errors.New("wait failed")
	complete := &directChildWait{done: make(chan struct{}), err: want}
	close(complete.done)
	if err := complete.await(time.Second); !errors.Is(err, want) {
		t.Fatalf("completed wait = %v", err)
	}
}
