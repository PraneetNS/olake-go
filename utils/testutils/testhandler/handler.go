// Package testhandler gives unit tests real databases and destinations in shared Docker containers.
// Each test gets an id that names what it creates, and cleanup removes everything under that id.
package testhandler

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"
)

const (
	idPrefix       = "olake_ut_"
	idAlphabet     = "abcdefghijklmnopqrstuvwxyz234567"
	cleanupTimeout = 2 * time.Minute
)

// Handler hands one test its isolated databases and destinations and removes them when the test ends.
type Handler struct {
	t       testing.TB
	id      string
	mu      sync.Mutex
	handles map[string]any
}

// New returns a handler with a fresh test id. No container starts until the test asks for one.
func New(t testing.TB) *Handler {
	t.Helper()
	h := &Handler{t: t, id: newID(), handles: map[string]any{}}
	t.Logf("testhandler: test id %s", h.id)
	return h
}

// ID names everything this test owns: its source databases and its destination database.
func (h *Handler) ID() string {
	return h.id
}

// newID returns a name valid as a database, a replication slot, an Iceberg namespace and a key prefix.
func newID() string {
	random := make([]byte, 12)
	_, _ = rand.Read(random)
	for i, b := range random {
		random[i] = idAlphabet[b&31]
	}
	return idPrefix + string(random)
}

// once returns the handle stored under key and creates it on the first call.
func once[T any](h *Handler, key string, create func() *T) *T {
	h.mu.Lock()
	defer h.mu.Unlock()
	if handle, ok := h.handles[key]; ok {
		return handle.(*T)
	}
	handle := create()
	h.handles[key] = handle
	return handle
}

// start returns the service's ready container and fails the test when Docker cannot provide it.
func (h *Handler) start(s *service) *container {
	h.t.Helper()
	ctx, cancel := h.untilDeadline()
	defer cancel()
	c, err := s.ensure(ctx, h.t)
	if err != nil {
		h.t.Fatalf("testhandler: %s", err)
	}
	return c
}

// untilDeadline is the test's context cut short just before the test binary's deadline,
// so a wait that cannot end fails with its own message instead of a timeout panic.
func (h *Handler) untilDeadline() (context.Context, context.CancelFunc) {
	if t, ok := h.t.(interface{ Deadline() (time.Time, bool) }); ok {
		if deadline, set := t.Deadline(); set {
			return context.WithDeadline(h.t.Context(), deadline.Add(-10*time.Second))
		}
	}
	return context.WithCancel(h.t.Context())
}

// cleanup registers a drop step for the end of the test, when the test's own context is already canceled.
func (h *Handler) cleanup(what string, drop func(ctx context.Context) error) {
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := drop(ctx); err != nil {
			h.t.Errorf("testhandler: cleanup of %s for test id %s failed: %s", what, h.id, err)
		}
	})
}
