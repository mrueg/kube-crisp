package tokencommand

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// Connections opened in the same instant share one run of the command.
//
// This is not a cache and must not become one: a command prints a token and
// says nothing about how long it is good for, so holding on to one would hand
// out a credential minted before whatever issued it had a chance to change its
// mind. What it does is share a run that is happening anyway, which is exactly
// what a pool refilling after an idle period produces — several connections
// opened at once, each of which would otherwise fork a process and ask a cloud
// for a token identical to the others'.
//
// The command here records every run and blocks until the test has released it,
// so the overlap is arranged rather than hoped for: the first caller is inside
// the command before any other caller starts.
func TestConnectionsOpenedTogetherShareOneRunOfTheCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the command this test installs is a shell script")
	}

	dir := t.TempDir()
	runs := filepath.Join(dir, "runs")
	gate := filepath.Join(dir, "gate")
	path := filepath.Join(dir, "token")
	body := "#!/bin/sh\n" +
		"echo run >> " + runs + "\n" +
		"while [ ! -f " + gate + " ]; do sleep 0.01; done\n" +
		"echo shared-token\n"
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil { //nolint:gosec // G306: a command has to be executable to be run
		t.Fatalf("installing the command returned error: %v", err)
	}

	c := &command{path: path, name: "token", timeout: maxTimeout}
	ctx := context.Background()

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		passwords []string
	)
	ask := func() {
		defer wg.Done()
		password, err := c.Password(ctx)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			t.Errorf("Password() returned error: %v", err)
			return
		}
		passwords = append(passwords, password)
	}

	wg.Add(1)
	go ask()

	// Wait until the command is running, so that what follows is a burst
	// arriving while it is, rather than a sequence of separate runs.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(runs); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the command never started")
		}
		time.Sleep(time.Millisecond)
	}

	const others = 7
	wg.Add(others)
	for range others {
		go ask()
	}

	// Let it finish, and let everybody waiting on it have the answer.
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("releasing the command returned error: %v", err)
	}
	wg.Wait()

	if len(passwords) != others+1 {
		t.Fatalf("%d of %d callers got a password", len(passwords), others+1)
	}
	for _, password := range passwords {
		if password != "shared-token" {
			t.Errorf("a caller got %q, want what the command printed", password)
		}
	}

	recorded, err := os.ReadFile(runs) //nolint:gosec // G304: a fixture inside the test's own temporary directory
	if err != nil {
		t.Fatalf("reading the record of runs returned error: %v", err)
	}
	if count := len(strings.Fields(string(recorded))); count != 1 {
		t.Errorf("the command ran %d times for one burst of connections, want once", count)
	}

	// And the run is not kept: the next connection asks again, because a
	// credential this provider held on to would be one nothing here can say is
	// still valid.
	if err := os.Remove(gate); err != nil {
		t.Fatalf("removing the gate returned error: %v", err)
	}
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("re-creating the gate returned error: %v", err)
	}
	if _, err := c.Password(ctx); err != nil {
		t.Fatalf("Password() returned error: %v", err)
	}
	recorded, err = os.ReadFile(runs) //nolint:gosec // G304: a fixture inside the test's own temporary directory
	if err != nil {
		t.Fatalf("reading the record of runs returned error: %v", err)
	}
	if count := len(strings.Fields(string(recorded))); count != 2 {
		t.Errorf("the command has run %d times; a connection opened after the burst has to ask again", count)
	}
}
