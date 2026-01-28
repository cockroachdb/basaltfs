// Directory operations for BasaltFS.
//
// BasaltFS uses a flat object namespace - directories are virtual constructs
// implemented by path prefix matching. This mirrors how object storage systems
// like S3 handle "directories".
//
// Implementation reference: See ../basalt-old/pkg/basalt for the prototype
// directory implementation.
package basaltfs

import "os"

// OpenDir opens a directory for reading.
// Implements vfs.FS.OpenDir.
func (fs *FS) OpenDir(name string) (DirHandle, error) {
	// TODO: Implement - return handle for listing.
	return &dirHandle{fs: fs, name: name}, nil
}

// DirHandle represents an open directory.
type DirHandle interface {
	// Sync syncs the directory (no-op for BasaltFS).
	Sync() error
	// Close closes the directory handle.
	Close() error
}

type dirHandle struct {
	fs   *FS
	name string
}

func (d *dirHandle) Sync() error {
	// Directory sync is a no-op in BasaltFS - object storage
	// doesn't have the same directory durability semantics.
	return nil
}

func (d *dirHandle) Close() error {
	return nil
}

// RemoveAll removes a directory and all its contents.
// Implements vfs.FS.RemoveAll.
func (fs *FS) RemoveAll(name string) error {
	// TODO: Implement:
	// 1. List all objects with this prefix
	// 2. Delete each object
	return nil
}

// Lock creates a lock file.
// Implements vfs.FS.Lock.
func (fs *FS) Lock(name string) (Lock, error) {
	// TODO: Implement using controller-based locking.
	return nil, os.ErrNotExist
}

// Lock represents an exclusive lock on a file.
type Lock interface {
	// Close releases the lock.
	Close() error
}
