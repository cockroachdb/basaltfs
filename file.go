// File handle implementation for BasaltFS.
//
// Implementation reference: See ../basalt-old/pkg/basalt for the prototype
// basaltFile implementation.
package basaltfs

import "io"

// File represents an open file in BasaltFS.
// It implements the pebble/vfs.File interface.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Closer

	// Sync ensures all written data is durably persisted.
	Sync() error

	// Stat returns file information.
	Stat() (FileInfo, error)
}

// FileInfo provides information about a file.
type FileInfo interface {
	// Size returns the file size in bytes.
	Size() int64
}

// writableFile is a file handle for writing.
type writableFile struct {
	fs   *FS
	name string
	// TODO: Add fields:
	// - Object ID
	// - Replica addresses
	// - Current offset
	// - Blob clients
}

func (f *writableFile) Write(p []byte) (n int, err error) {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Append to all replicas in parallel
	// 2. Wait for all to succeed
	// 3. Update offset
	return 0, nil
}

func (f *writableFile) Sync() error {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Sync on all replicas in parallel
	// 2. Wait for all to succeed
	return nil
}

func (f *writableFile) Close() error {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Seal on all replicas
	// 2. Update controller with final size
	return nil
}

func (f *writableFile) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (f *writableFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, io.EOF
}

func (f *writableFile) Stat() (FileInfo, error) {
	return nil, nil
}

// readableFile is a file handle for reading.
type readableFile struct {
	fs     *FS
	name   string
	offset int64
	// TODO: Add fields:
	// - Object ID
	// - Replica addresses
	// - Object size
	// - Preferred replica for reads
}

func (f *readableFile) Read(p []byte) (n int, err error) {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Read from preferred replica
	// 2. Update offset
	return 0, io.EOF
}

func (f *readableFile) ReadAt(p []byte, off int64) (n int, err error) {
	// TODO: Implement - see basalt-old/pkg/basalt prototype:
	// 1. Read from preferred replica at offset
	return 0, io.EOF
}

func (f *readableFile) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}

func (f *readableFile) Sync() error {
	return nil
}

func (f *readableFile) Close() error {
	return nil
}

func (f *readableFile) Stat() (FileInfo, error) {
	return nil, nil
}
