// Directory operations for BasaltFS.
//
// BasaltFS uses a flat object namespace - directories are virtual constructs
// implemented by path prefix matching. This mirrors how object storage systems
// like S3 handle "directories".
package basaltfs

import (
	"io"
	"path/filepath"

	"github.com/cockroachdb/pebble/vfs"
)

// dirHandle represents an open directory. It implements vfs.File but only
// supports Sync() and Close() - other operations return errors.
type dirHandle struct {
	fs   *FS
	name string
}

var _ vfs.File = (*dirHandle)(nil)

// Read is not supported for directories.
func (d *dirHandle) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

// ReadAt is not supported for directories.
func (d *dirHandle) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, io.EOF
}

// Write is not supported for directories.
func (d *dirHandle) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}

// WriteAt is not supported for directories.
func (d *dirHandle) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, io.ErrClosedPipe
}

// Sync is a no-op for directories in basaltfs.
func (d *dirHandle) Sync() error {
	// Directory sync is a no-op in BasaltFS - object storage
	// doesn't have the same directory durability semantics.
	return nil
}

// SyncData is a no-op for directories.
func (d *dirHandle) SyncData() error {
	return nil
}

// SyncTo is a no-op for directories.
func (d *dirHandle) SyncTo(length int64) (fullSync bool, err error) {
	return false, nil
}

// Close closes the directory handle.
func (d *dirHandle) Close() error {
	return nil
}

// Stat returns file information for the directory.
func (d *dirHandle) Stat() (vfs.FileInfo, error) {
	return &fileInfo{
		name:  filepath.Base(d.name),
		size:  0,
		isDir: true,
	}, nil
}

// Fd returns InvalidFd since directories are not backed by OS files.
func (d *dirHandle) Fd() uintptr {
	return vfs.InvalidFd
}

// Preallocate is a no-op for directories.
func (d *dirHandle) Preallocate(offset, length int64) error {
	return nil
}

// Prefetch is a no-op for directories.
func (d *dirHandle) Prefetch(offset int64, length int64) error {
	return nil
}

// noopCloser is a no-op implementation of io.Closer used for Lock().
type noopCloser struct{}

func (noopCloser) Close() error {
	return nil
}
