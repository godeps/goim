package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestFileStoreRoundTripIntegrityAndPermissions(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw := []byte("{\n  \"message\": \"spaces remain exact\"\n}")
	if ok, err := store.CompareAndSwap(ctx, "journal", 0, raw); err != nil || !ok {
		t.Fatalf("write: %v %v", ok, err)
	}
	read, err := store.Load(ctx, "journal")
	if err != nil || read.Version != 1 || !bytes.Equal(raw, read.Data) {
		t.Fatalf("round trip: %+v %v", read, err)
	}
	read.Data[0] = 'x'
	again, err := store.Load(ctx, "journal")
	if err != nil || !bytes.Equal(again.Data, raw) {
		t.Fatal("mutable alias escaped")
	}
	if ok, err := store.CompareAndSwap(ctx, "journal", 0, []byte("{}")); err != nil || ok {
		t.Fatalf("stale CAS: %v %v", ok, err)
	}
	if err = os.WriteFile(store.filename("journal"), []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Load(ctx, "journal"); err == nil {
		t.Fatal("corruption silently accepted")
	}
	if ok, err := store.CompareAndSwap(ctx, "journal", 0, []byte("{}")); err == nil || ok {
		t.Fatal("corruption reset uncertain history")
	}
	if err = os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err = NewFileStore(dir); err == nil {
		t.Fatal("public state directory accepted")
	}
}

func TestFileStoreProcessHelper(t *testing.T) {
	dir := os.Getenv("GOIM_REMOTE_TEST_STORE_DIR")
	if dir == "" {
		return
	}
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		for {
			old, err := store.Load(ctx, "cross-process")
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			if old.Version > 0 {
				if err = json.Unmarshal(old.Data, &count); err != nil {
					t.Fatal(err)
				}
			}
			data, _ := json.Marshal(count + 1)
			ok, err := store.CompareAndSwap(ctx, "cross-process", old.Version, data)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				break
			}
		}
	}
}
func TestFileStoreCASAcrossProcesses(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd", "openbsd", "netbsd", "dragonfly":
	default:
		t.Skip("FileStore OS locks are unavailable on this platform")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan string, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestFileStoreProcessHelper$")
			cmd.Env = append(os.Environ(), "GOIM_REMOTE_TEST_STORE_DIR="+dir)
			output, err := cmd.CombinedOutput()
			if err != nil {
				failures <- err.Error() + ": " + string(output)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(context.Background(), "cross-process")
	if err != nil {
		t.Fatal(err)
	}
	count, err := strconv.Atoi(string(got.Data))
	if err != nil || count != 80 || got.Version != 80 {
		t.Fatalf("lost cross-process updates: %+v %v", got, err)
	}
}
