package goim

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// maxLogFileSize is the max size of a single log file (100 MB).
	maxLogFileSize = 100 * 1024 * 1024
	// maxTotalLogSize is the max total size of all log files (1 GB).
	maxTotalLogSize = 1 * 1024 * 1024 * 1024
)

// rotatingWriter is an io.Writer that automatically rotates log files
// when they exceed maxLogFileSize and prunes old files when total size
// exceeds maxTotalLogSize.
type rotatingWriter struct {
	mu      sync.Mutex
	dir     string
	prefix  string
	file    *os.File
	written int64
}

func newRotatingWriter(dir, prefix string) (*rotatingWriter, error) {
	w := &rotatingWriter{dir: dir, prefix: prefix}
	if err := w.openNew(); err != nil {
		return nil, err
	}
	// Prune old files on startup.
	w.prune()
	return w, nil
}

func (w *rotatingWriter) openNew() error {
	name := fmt.Sprintf("%s-%s.log", w.prefix, time.Now().Format("20060102-150405"))
	path := filepath.Join(w.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	info, _ := f.Stat()
	w.file = f
	if info != nil {
		w.written = info.Size()
	} else {
		w.written = 0
	}
	return nil
}

func (w *rotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.written+int64(len(p)) > maxLogFileSize {
		w.file.Close()
		if err := w.openNew(); err != nil {
			return 0, err
		}
		w.prune()
	}

	n, err := w.file.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *rotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// prune removes oldest log files until total size is under maxTotalLogSize.
func (w *rotatingWriter) prune() {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}

	type logFile struct {
		name string
		size int64
	}
	var files []logFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), w.prefix) || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, logFile{name: e.Name(), size: info.Size()})
	}

	// Sort oldest first.
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })

	var total int64
	for _, f := range files {
		total += f.size
	}

	for len(files) > 1 && total > maxTotalLogSize {
		oldest := files[0]
		os.Remove(filepath.Join(w.dir, oldest.name))
		total -= oldest.size
		files = files[1:]
	}
}

// SetupIMLogger configures slog to write IM bridge logs to rotated JSON files
// under ~/.animus/logs/. Logs are NOT written to the terminal.
// Returns a cleanup function that restores the original slog handler and
// closes the log file.
func SetupIMLogger() (cleanup func(), err error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}

	logDir := filepath.Join(home, ".animus", "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	w, err := newRotatingWriter(logDir, "im")
	if err != nil {
		return nil, fmt.Errorf("open im log: %w", err)
	}

	prev := slog.Default()
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))

	return func() {
		slog.SetDefault(prev)
		w.Close()
	}, nil
}
