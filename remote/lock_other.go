//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package remote

import (
	"context"
	"errors"
)

func fileLock(context.Context, string) (func(), error) {
	return nil, errors.New("FileStore locking is unsupported on this OS; use SQLStore")
}
