package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godeps/goim/remote"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func TestSQLiteDurableCASAndProtocol(t *testing.T) {
	name := filepath.Join(t.TempDir(), "state.sqlite")
	testSQL(t, "sqlite", "file:"+name+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
}
func TestPostgresDurableCASAndProtocol(t *testing.T) {
	dsn := os.Getenv("GOIM_REMOTE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set GOIM_REMOTE_TEST_POSTGRES_DSN to an isolated disposable database")
	}
	testSQL(t, "postgres", dsn)
}
func testSQL(t *testing.T, dialect, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	open := func() *sql.DB {
		driver := dialect
		if dialect == "postgres" {
			driver = "pgx"
		}
		db, err := sql.Open(driver, dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(8)
		if err = db.PingContext(ctx); err != nil {
			db.Close()
			t.Fatal(err)
		}
		return db
	}
	db1, db2 := open(), open()
	defer db1.Close()
	defer db2.Close()
	table := "goim_remote_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	a, err := remote.NewSQLStore(db1, dialect, table)
	if err != nil {
		t.Fatal(err)
	}
	b, err := remote.NewSQLStore(db2, dialect, table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db1.ExecContext(ctx, a.SchemaSQL()); err != nil {
		t.Fatal(err)
	}
	defer db1.ExecContext(context.Background(), "DROP TABLE "+table)
	missing, err := a.Load(ctx, "missing")
	if err != nil || missing.Version != 0 {
		t.Fatalf("missing: %+v %v", missing, err)
	}
	initial := []byte(`{"count":0}`)
	if ok, err := a.CompareAndSwap(ctx, "counter", 0, initial); err != nil || !ok {
		t.Fatalf("insert: %v %v", ok, err)
	}
	if ok, err := b.CompareAndSwap(ctx, "counter", 0, initial); err != nil || ok {
		t.Fatalf("duplicate insert: %v %v", ok, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			store := a
			if worker%2 == 1 {
				store = b
			}
			for i := 0; i < 12; i++ {
				for {
					old, err := store.Load(ctx, "counter")
					if err != nil {
						errs <- err
						return
					}
					var value struct {
						Count int `json:"count"`
					}
					if err = json.Unmarshal(old.Data, &value); err != nil {
						errs <- err
						return
					}
					value.Count++
					data, _ := json.Marshal(value)
					ok, err := store.CompareAndSwap(ctx, "counter", old.Version, data)
					if err != nil {
						errs <- err
						return
					}
					if ok {
						break
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	final, err := a.Load(ctx, "counter")
	if err != nil {
		t.Fatal(err)
	}
	var count struct {
		Count int `json:"count"`
	}
	json.Unmarshal(final.Data, &count)
	if count.Count != 192 || final.Version != 193 {
		t.Fatalf("CAS lost updates: %+v v%d", count, final.Version)
	}

	svc1, err := remote.NewService(a, remote.Options{Namespace: "integration"})
	if err != nil {
		t.Fatal(err)
	}
	svc2, err := remote.NewService(b, remote.Options{Namespace: "integration"})
	if err != nil {
		t.Fatal(err)
	}
	owner := remote.Principal{ID: "alice"}
	pair, err := svc1.CreatePairing(ctx, owner, remote.PairingRequest{})
	if err != nil {
		t.Fatal(err)
	}
	req, creds, err := remote.NewEnrollment(pair.Code, "laptop", []remote.Agent{{Name: "codex"}}, []remote.Workspace{{ID: "repo", Name: "Repository"}})
	if err != nil {
		t.Fatal(err)
	}
	req.ExpectedPrincipal = owner.ID
	if _, err = svc2.Enroll(ctx, req); err != nil {
		t.Fatal(err)
	}
	key, err := remote.NewRunKey()
	if err != nil {
		t.Fatal(err)
	}
	create := remote.CreateRunRequest{IdempotencyKey: key, DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "test CAS"}
	ids := make(chan string, 16)
	errs = make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc := svc1
			if i%2 == 1 {
				svc = svc2
			}
			run, err := svc.CreateRun(ctx, owner, create)
			if err != nil {
				errs <- err
				return
			}
			ids <- run.ID
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	id := ""
	for got := range ids {
		if id == "" {
			id = got
		}
		if id != got {
			t.Fatal("duplicate run under concurrent CAS")
		}
	}
	var firstClaims atomic.Int32
	errs = make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc := svc1
			if i%2 == 1 {
				svc = svc2
			}
			claim, err := remote.NewPollID()
			if err != nil {
				errs <- err
				return
			}
			assignment, err := svc.Poll(ctx, creds.Token, remote.PollRequest{ID: claim})
			if err != nil {
				errs <- err
				return
			}
			if assignment.Assignment == nil || assignment.Assignment.Run.ID != id {
				errs <- fmt.Errorf("assignment differs")
				return
			}
			if assignment.Assignment.New {
				firstClaims.Add(1)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if firstClaims.Load() != 1 {
		t.Fatalf("concurrent first claims: %d", firstClaims.Load())
	}
	run, err := svc1.Run(ctx, owner, id)
	if err != nil {
		t.Fatal(err)
	}
	batch := remote.EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []remote.Event{{Seq: 1, Type: remote.EventStarted}, {Seq: 2, Type: remote.EventCompleted, Text: "done"}}}
	receipt, err := svc2.AppendEvents(ctx, creds.Token, id, batch)
	if err != nil || receipt.AckSeq != 2 {
		t.Fatalf("event commit: %+v %v", receipt, err)
	}
	db2.Close()
	db2 = open()
	defer db2.Close()
	restartedStore, err := remote.NewSQLStore(db2, dialect, table)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := remote.NewService(restartedStore, remote.Options{Namespace: "integration"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = restarted.AppendEvents(ctx, creds.Token, id, batch)
	if err != nil || receipt.AckSeq != 2 || receipt.State != remote.StateSucceeded {
		t.Fatalf("durable ACK after reconnect: %+v %v", receipt, err)
	}
	_, err = restarted.Run(ctx, remote.Principal{ID: "bob"}, id)
	if remote.ErrorCode(err) != "not_found" {
		t.Fatalf("principal isolation: %v", err)
	}
	if err = restarted.RevokeDevice(ctx, owner, creds.DeviceID); err != nil {
		t.Fatal(err)
	}
	if _, err = svc1.Device(ctx, creds.Token); remote.ErrorCode(err) != "unauthorized" {
		t.Fatalf("cross-instance revocation: %v", err)
	}
	if err = db1.PingContext(ctx); err != nil {
		t.Fatalf("borrowed host database closed: %v", err)
	}
}
