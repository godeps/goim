package remote

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
)

// Snapshot is an immutable state value with a compare-and-swap revision.
// A missing key has Version 0. Implementations must copy returned byte slices.
type Snapshot struct {
	Version uint64
	Data    []byte
}

// Store must provide atomic durable CAS across every process sharing it.
// CompareAndSwap is a single commit, never a read followed by an unconditional
// write. A successful return means the new value survives a process restart.
type Store interface {
	Load(context.Context, string) (Snapshot, error)
	CompareAndSwap(context.Context, string, uint64, []byte) (bool, error)
}

// SQLStore borrows a caller-owned database. It never closes or migrates it.
type SQLStore struct {
	db             *sql.DB
	dialect, table string
}

var sqlName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// NewSQLStore accepts sqlite/sqlite3 or postgres/postgresql. Run SchemaSQL through
// the host's explicit migrations before use. No external SQL driver is imported.
func NewSQLStore(db *sql.DB, dialect, table string) (*SQLStore, error) {
	if db == nil || !sqlName.MatchString(table) {
		return nil, errors.New("invalid remote SQL store configuration")
	}
	switch dialect {
	case "sqlite", "sqlite3":
		dialect = "sqlite"
	case "postgres", "postgresql":
		dialect = "postgres"
	default:
		return nil, errors.New("unsupported remote SQL dialect")
	}
	return &SQLStore{db: db, dialect: dialect, table: table}, nil
}

// SchemaSQL is portable DDL for an explicit host migration.
func (s *SQLStore) SchemaSQL() string {
	return "CREATE TABLE IF NOT EXISTS " + s.table + " (\"key\" TEXT PRIMARY KEY, version BIGINT NOT NULL, payload TEXT NOT NULL)"
}
func (s *SQLStore) bind(n int) string {
	if s.dialect == "postgres" {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}
func (s *SQLStore) Load(ctx context.Context, key string) (Snapshot, error) {
	var version int64
	var raw string
	err := s.db.QueryRowContext(ctx, "SELECT version,payload FROM "+s.table+" WHERE \"key\"="+s.bind(1), key).Scan(&version, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	if version <= 0 || len(raw) > 64<<20 {
		return Snapshot{}, errors.New("corrupt remote SQL state")
	}
	return Snapshot{Version: uint64(version), Data: []byte(raw)}, nil
}
func (s *SQLStore) CompareAndSwap(ctx context.Context, key string, version uint64, data []byte) (bool, error) {
	if version >= math.MaxInt64 || len(data) > 64<<20 {
		return false, errors.New("remote state capacity exceeded")
	}
	var result sql.Result
	var err error
	if version == 0 {
		result, err = s.db.ExecContext(ctx, "INSERT INTO "+s.table+" (\"key\",version,payload) VALUES ("+s.bind(1)+",1,"+s.bind(2)+") ON CONFLICT (\"key\") DO NOTHING", key, string(data))
	} else {
		result, err = s.db.ExecContext(ctx, "UPDATE "+s.table+" SET version=version+1,payload="+s.bind(1)+" WHERE \"key\"="+s.bind(2)+" AND version="+s.bind(3), string(data), key, int64(version))
	}
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// FileStore is a bounded, private durable CAS store for local single-host use.
// Linux/macOS use an OS lock across processes. For multiple hosts use SQLStore.
type FileStore struct{ dir string }
type fileEnvelope struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
	Digest  string `json:"digest"`
	Data    []byte `json:"data"`
}

// NewFileStore creates/opens an owner-only directory. Corruption is fail-closed;
// it never silently resets a journal or retries an uncertain execution.
func NewFileStore(dir string) (*FileStore, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(absolute, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("remote store directory must be private")
	}
	return &FileStore{dir: absolute}, nil
}
func (s *FileStore) filename(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".json")
}
func (s *FileStore) Load(ctx context.Context, key string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	name := s.filename(key)
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 65<<20 {
		return Snapshot{}, errors.New("invalid private remote state")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		return Snapshot{}, err
	}
	var e fileEnvelope
	if err = json.Unmarshal(raw, &e); err != nil || e.Key != key || e.Version == 0 || !json.Valid(e.Data) {
		return Snapshot{}, errors.New("corrupt remote state")
	}
	sum := sha256.Sum256(e.Data)
	if hex.EncodeToString(sum[:]) != e.Digest {
		return Snapshot{}, errors.New("remote state integrity mismatch")
	}
	return Snapshot{Version: e.Version, Data: append([]byte(nil), e.Data...)}, nil
}
func (s *FileStore) CompareAndSwap(ctx context.Context, key string, version uint64, data []byte) (bool, error) {
	if len(data) > 48<<20 || !json.Valid(data) || version == math.MaxUint64 {
		return false, errors.New("invalid remote state value")
	}
	unlock, err := fileLock(ctx, s.filename(key)+".lock")
	if err != nil {
		return false, err
	}
	defer unlock()
	old, err := s.Load(ctx, key)
	if err != nil {
		return false, err
	}
	if old.Version != version {
		return false, nil
	}
	sum := sha256.Sum256(data)
	raw, err := json.Marshal(fileEnvelope{Key: key, Version: version + 1, Digest: hex.EncodeToString(sum[:]), Data: append([]byte(nil), data...)})
	if err != nil {
		return false, err
	}
	f, err := os.CreateTemp(s.dir, ".remote-write-")
	if err != nil {
		return false, err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, err = f.Write(raw); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = os.Rename(temp, s.filename(key)); err != nil {
		return false, err
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return true, err
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return true, err
	}
	return true, nil
}
