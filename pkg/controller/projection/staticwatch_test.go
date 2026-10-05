package projection

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/fsnotify/fsnotify"
)

// The watch has to cover what the loader reads, links included.
//
// Walked on its own, the watch looked at a symlinked root rather than through
// it and added nothing, so a --projection-dir pointing at `current ->
// releases/42` was never watched; and it stopped at a symlinked subdirectory
// the loader now reads. Dotted directories stay unwatched, as they stay unread.
// The link's parent is watched too, since swapping the link is an event there
// and nowhere else.
func TestWatchStaticTreeFollowsWhatTheLoaderReads(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolving the temporary directory: %v", err)
	}
	release := filepath.Join(base, "releases", "42")
	shared := filepath.Join(base, "shared")
	for _, dir := range []string{
		filepath.Join(release, "pagila"),
		filepath.Join(release, ".git"),
		shared,
	} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
	}
	if err := os.Symlink(shared, filepath.Join(release, "shared")); err != nil {
		t.Fatalf("linking shared: %v", err)
	}
	current := filepath.Join(base, "current")
	if err := os.Symlink(release, current); err != nil {
		t.Fatalf("linking current: %v", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatalf("creating a watcher: %v", err)
	}
	defer func() { _ = watcher.Close() }()

	c := &Controller{staticDir: current}
	if err := c.watchStaticTree(watcher); err != nil {
		t.Fatalf("watchStaticTree: %v", err)
	}

	got := watcher.WatchList()
	sort.Strings(got)
	// base is there for the root link: swapping it changes nothing under the
	// directory it pointed at, so only its parent sees the swap.
	want := []string{base, release, filepath.Join(release, "pagila"), shared}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("watching %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("watching %v, want %v", got, want)
		}
	}
}
