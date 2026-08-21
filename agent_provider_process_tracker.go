package piacp

import (
	"context"
	"errors"
	"sync"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

type providerProcessInventory interface {
	ProviderDescendantCount() (int, bool)
}

type providerProcessTracker struct {
	mu         sync.Mutex
	hooks      RuntimeResourceHooks
	nextID     uint64
	entries    map[uint64]providerProcessEntry
	publishing bool
	dirty      bool
}

type providerProcessEntry struct {
	inventory providerProcessInventory
}

type providerProcessRoot struct {
	tracker *providerProcessTracker
	id      uint64
}

func newProviderProcessTracker(hooks RuntimeResourceHooks) *providerProcessTracker {
	return &providerProcessTracker{
		hooks:   hooks,
		entries: make(map[uint64]providerProcessEntry),
	}
}

func (t *providerProcessTracker) register() *providerProcessRoot {
	root := t.registerDeferred()
	t.update(context.Background(), root.id, providerProcessEntry{})

	return root
}

// registerDeferred records ownership without invoking an observation hook.
// Spawn callers use it so they can attach the returned root, process, and
// client to their construction owner before any observer can block or panic.
func (t *providerProcessTracker) registerDeferred() *providerProcessRoot {
	t.mu.Lock()
	t.nextID++
	t.entries[t.nextID] = providerProcessEntry{}
	root := &providerProcessRoot{tracker: t, id: t.nextID}
	t.mu.Unlock()

	return root
}

func (r *providerProcessRoot) observe(ctx context.Context, process any) {
	inventory, ok := process.(providerProcessInventory)
	if ok {
		r.tracker.update(ctx, r.id, providerProcessEntry{inventory: inventory})

		return
	}

	r.tracker.update(ctx, r.id, providerProcessEntry{})
}

func (r *providerProcessRoot) retire(ctx context.Context, complete bool) {
	if !complete {
		return
	}

	r.tracker.remove(ctx, r.id)
}

func (t *providerProcessTracker) update(ctx context.Context, id uint64, entry providerProcessEntry) {
	t.mu.Lock()
	if _, ok := t.entries[id]; !ok {
		t.mu.Unlock()

		return
	}

	t.entries[id] = entry
	startPublisher := t.markDirtyLocked()
	t.mu.Unlock()

	if startPublisher {
		t.publish(ctx)
	}
}

func (t *providerProcessTracker) remove(ctx context.Context, id uint64) {
	t.mu.Lock()
	if _, ok := t.entries[id]; !ok {
		t.mu.Unlock()

		return
	}

	delete(t.entries, id)
	startPublisher := t.markDirtyLocked()
	t.mu.Unlock()

	if startPublisher {
		t.publish(ctx)
	}
}

func (t *providerProcessTracker) markDirtyLocked() bool {
	t.dirty = true
	if t.publishing {
		return false
	}

	t.publishing = true

	return true
}

func (t *providerProcessTracker) publish(ctx context.Context) {
	for {
		t.mu.Lock()
		if !t.dirty {
			t.publishing = false
			t.mu.Unlock()

			return
		}

		t.dirty = false
		total, available := t.snapshotLocked()
		t.mu.Unlock()

		if available {
			observeRuntimeProcessSnapshot(ctx, t.hooks, RuntimeProcessProviderDescendant, total)
		}
	}
}

func (t *providerProcessTracker) snapshotLocked() (int, bool) {
	total := 0

	for _, entry := range t.entries {
		if entry.inventory == nil {
			return 0, false
		}

		count, available := entry.inventory.ProviderDescendantCount()
		if !available || count < 0 {
			return 0, false
		}

		total += count
	}

	return total, true
}

func providerProcessTreeComplete(err error) bool {
	return !errors.Is(err, internalpi.ErrProcessContainmentIncomplete)
}
