// Package basaltfs provides a Pebble VFS implementation backed by Basalt storage.
//
// BasaltFS implements the pebble/vfs.FS interface, storing all file data as
// objects in Basalt blob servers. It coordinates with the Basalt controller
// for object placement and metadata management.
//
// Usage:
//
//	ctrl := basaltclient.NewControllerClient(addr)
//	fs := basaltfs.NewFS(ctrl, basaltfs.Options{})
//	pebbleOpts := &pebble.Options{FS: fs}
//
// Implementation reference: See ../basalt-old/pkg/basalt for the prototype
// BasaltVFS implementation.
package basaltfs

import (
	"os"

	"github.com/cockroachdb/basaltclient"
)

// Options configures the BasaltFS behavior.
type Options struct {
	// Replication is the number of replicas for each object.
	// Default: 3
	Replication int

	// LocalAZ is this node's availability zone, used to prefer local replicas for reads.
	LocalAZ string
}

// FS implements pebble/vfs.FS backed by Basalt storage.
type FS struct {
	ctrl *basaltclient.ControllerClient
	opts Options

	// TODO: Add internal state:
	// - Mount ID from controller
	// - Blob client pool
	// - In-memory metadata cache
}

// NewFS creates a new BasaltFS using the given controller client.
func NewFS(ctrl *basaltclient.ControllerClient, opts Options) (*FS, error) {
	if opts.Replication == 0 {
		opts.Replication = 3
	}
	// TODO: Register mount with controller.
	return &FS{
		ctrl: ctrl,
		opts: opts,
	}, nil
}

// Close releases resources associated with the filesystem.
func (fs *FS) Close() error {
	// TODO: Unmount from controller.
	return nil
}

// Create creates a new file for writing.
// Implements vfs.FS.Create.
func (fs *FS) Create(name string) (File, error) {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Request object creation from controller
	// 2. Create object on all replica blob servers
	// 3. Return writable file handle
	return nil, os.ErrNotExist
}

// Open opens a file for reading.
// Implements vfs.FS.Open.
func (fs *FS) Open(name string) (File, error) {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Lookup object metadata from controller
	// 2. Return readable file handle
	return nil, os.ErrNotExist
}

// Remove deletes a file.
// Implements vfs.FS.Remove.
func (fs *FS) Remove(name string) error {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Delete object via controller
	// 2. Controller coordinates deletion from blob servers
	return nil
}

// Rename renames a file.
// Implements vfs.FS.Rename.
func (fs *FS) Rename(oldname, newname string) error {
	// TODO: Implement - metadata-only operation in controller.
	return nil
}

// Link creates a hard link.
// Implements vfs.FS.Link.
func (fs *FS) Link(oldname, newname string) error {
	// TODO: Implement - metadata-only operation in controller.
	return nil
}

// MkdirAll creates a directory and all parent directories.
// Implements vfs.FS.MkdirAll.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	// Directories are virtual in BasaltFS - no-op.
	return nil
}

// Stat returns file info.
// Implements vfs.FS.Stat.
func (fs *FS) Stat(name string) (os.FileInfo, error) {
	// TODO: Implement - lookup from controller.
	return nil, os.ErrNotExist
}

// List returns the names of files in a directory.
// Implements vfs.FS.List.
func (fs *FS) List(dir string) ([]string, error) {
	// TODO: Implement - list objects from controller.
	return nil, nil
}
