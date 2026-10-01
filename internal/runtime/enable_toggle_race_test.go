package runtime

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
)

// Concurrent server toggles (enable, quarantine, bulk enable) for DIFFERENT servers must not
// lose each other's update. Each toggle does storage write → SaveConfiguration (storage → in-memory cfg) →
// LoadConfiguredServers (in-memory cfg → storage, "config as source of truth"). Without
// serialization, one call's save can capture storage before another call's write and then
// its reload writes that stale value back. Seen live on 2026-09-30: two enables 9ms apart,
// both returned success, one server ended disabled.

func newToggleRaceRuntime(t *testing.T, n int, enabled bool) *Runtime {
	t.Helper()
	tmpDir := t.TempDir()
	cfg := &config.Config{DataDir: tmpDir}
	for i := 0; i < n; i++ {
		cfg.Servers = append(cfg.Servers, &config.ServerConfig{
			Name:     fmt.Sprintf("srv-%d", i),
			URL:      fmt.Sprintf("http://127.0.0.1:1/%d", i), // never connects
			Protocol: "http",
			Enabled:  enabled,
		})
	}

	rt, err := New(cfg, filepath.Join(tmpDir, "mcp_config.json"), zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.LoadConfiguredServers(nil); err != nil {
		t.Fatalf("initial load: %v", err)
	}
	return rt
}

// runConcurrently runs each fn in its own goroutine and reports any returned errors.
func runConcurrently(t *testing.T, fns ...func() error) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, len(fns))
	for _, fn := range fns {
		wg.Add(1)
		go func(fn func() error) {
			defer wg.Done()
			if err := fn(); err != nil {
				errs <- err
			}
		}(fn)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("toggle returned error: %v", err)
	}
}

// assertAllServers checks the given predicate against both storage and the in-memory config.
func assertAllServers(t *testing.T, rt *Runtime, what string, ok func(*config.ServerConfig) bool) {
	t.Helper()
	stored, err := rt.storageManager.ListUpstreamServers()
	if err != nil {
		t.Fatalf("list stored servers: %v", err)
	}
	for _, s := range stored {
		if !ok(s) {
			t.Errorf("storage: %s is not %s after a successful concurrent toggle", s.Name, what)
		}
	}
	for _, s := range rt.Config().Servers {
		if !ok(s) {
			t.Errorf("in-memory config: %s is not %s after a successful concurrent toggle", s.Name, what)
		}
	}
}

func TestEnableServer_ConcurrentTogglesOfDifferentServersAllPersist(t *testing.T) {
	const n = 8
	rt := newToggleRaceRuntime(t, n, false)

	var fns []func() error
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("srv-%d", i)
		fns = append(fns, func() error { return rt.EnableServer(name, true) })
	}
	runConcurrently(t, fns...)

	assertAllServers(t, rt, "enabled", func(s *config.ServerConfig) bool { return s.Enabled })
}

func TestQuarantineServer_ConcurrentTogglesOfDifferentServersAllPersist(t *testing.T) {
	const n = 8
	rt := newToggleRaceRuntime(t, n, false)

	var fns []func() error
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("srv-%d", i)
		fns = append(fns, func() error { return rt.QuarantineServer(name, true) })
	}
	runConcurrently(t, fns...)

	assertAllServers(t, rt, "quarantined", func(s *config.ServerConfig) bool { return s.Quarantined })
}

// A bulk enable racing single enables of the other servers must not drop either side.
func TestBulkEnableServers_ConcurrentWithSingleEnablesAllPersist(t *testing.T) {
	const n = 8
	rt := newToggleRaceRuntime(t, n, false)

	var bulk []string
	var fns []func() error
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("srv-%d", i)
		if i%2 == 0 {
			bulk = append(bulk, name)
			continue
		}
		fns = append(fns, func() error { return rt.EnableServer(name, true) })
	}
	fns = append(fns, func() error {
		perServer, err := rt.BulkEnableServers(bulk, true)
		for name, e := range perServer {
			t.Errorf("bulk enable %s: %v", name, e)
		}
		return err
	})
	runConcurrently(t, fns...)

	assertAllServers(t, rt, "enabled", func(s *config.ServerConfig) bool { return s.Enabled })
}
