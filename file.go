// File handle implementation for BasaltFS.
package basaltfs

import (
	"context"
	"io"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/basaltclient"
	"github.com/cockroachdb/basaltclient/basaltpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// file represents an open file in basaltfs. It implements the vfs.File interface
// and can be configured for read-only, write-only, or read-write access.
type file struct {
	fs        *FS
	name      string
	objectID  basaltclient.ObjectID
	replicas  []*basaltpb.ReplicaInfo // data addresses + zones
	readable  bool
	writable  bool
	walWriter *basaltclient.QuorumWriter // non-nil for WAL files

	mu struct {
		sync.Mutex
		size         int64
		offset       int64 // current read/write offset for sequential operations
		closed       bool
		syncedOffset int64
		buf          []byte // write buffer for non-WAL files
	}
}

var _ vfs.File = (*file)(nil)

// Read reads up to len(p) bytes from the file.
// Implements io.Reader.
func (f *file) Read(p []byte) (n int, err error) {
	if !f.readable {
		return 0, errors.New("file not opened for reading")
	}

	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return 0, errors.New("file is closed")
	}
	offset := f.mu.offset
	size := f.mu.size
	f.mu.Unlock()

	if offset >= size {
		return 0, io.EOF
	}

	// Limit read to remaining bytes.
	remaining := size - offset
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err = f.ReadAt(p, offset)

	f.mu.Lock()
	f.mu.offset += int64(n)
	f.mu.Unlock()

	return n, err
}

// ReadAt reads len(p) bytes from the file at the given offset.
// Implements io.ReaderAt.
func (f *file) ReadAt(p []byte, off int64) (n int, err error) {
	if !f.readable {
		return 0, errors.New("file not opened for reading")
	}

	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return 0, errors.New("file is closed")
	}
	size := f.mu.size
	f.mu.Unlock()

	if off >= size {
		return 0, io.EOF
	}

	// Select a replica for reading, preferring local AZ.
	replica := f.fs.selectReadReplica(f.replicas)
	if replica == "" {
		return 0, errors.New("no replicas available")
	}

	// Acquire a data client from the pool.
	client := f.fs.dataPool.Acquire(replica)
	if client == nil {
		return 0, errors.Newf("failed to acquire data client for %s", replica)
	}

	// Read from the replica.
	n, err = client.Read(f.objectID, uint64(off), p)
	if err != nil {
		f.fs.dataPool.ReleaseWithError(client)
		return n, err
	}

	f.fs.dataPool.Release(client)

	// Check if we hit EOF.
	if off+int64(n) >= size {
		return n, io.EOF
	}

	return n, nil
}

// Write writes len(p) bytes to the file.
// Implements io.Writer.
func (f *file) Write(p []byte) (n int, err error) {
	if !f.writable {
		return 0, errors.New("file not opened for writing")
	}

	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return 0, errors.New("file is closed")
	}
	offset := f.mu.offset
	f.mu.Unlock()

	n, err = f.writeAtLocked(p, offset)

	f.mu.Lock()
	f.mu.offset += int64(n)
	if f.mu.offset > f.mu.size {
		f.mu.size = f.mu.offset
	}
	f.mu.Unlock()

	return n, err
}

// WriteAt writes len(p) bytes to the file at the given offset.
// Implements io.WriterAt.
func (f *file) WriteAt(p []byte, off int64) (n int, err error) {
	if !f.writable {
		return 0, errors.New("file not opened for writing")
	}

	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return 0, errors.New("file is closed")
	}
	f.mu.Unlock()

	n, err = f.writeAtLocked(p, off)

	f.mu.Lock()
	endOffset := off + int64(n)
	if endOffset > f.mu.size {
		f.mu.size = endOffset
	}
	f.mu.Unlock()

	return n, err
}

// writeAtLocked performs the actual write operation.
func (f *file) writeAtLocked(p []byte, off int64) (n int, err error) {
	if f.walWriter != nil {
		// WAL files use the QuorumWriter which handles all writes internally.
		// QuorumWriter only supports sequential appends at its tracked offset,
		// so we append the data and let it manage the offset.
		if err := f.walWriter.WriteAndSync(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}

	// Non-WAL files: append to all replicas.
	var wg sync.WaitGroup
	errs := make([]error, len(f.replicas))

	for i, r := range f.replicas {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()
			client := f.fs.dataPool.Acquire(addr)
			if client == nil {
				errs[idx] = errors.Newf("failed to acquire data client for %s", addr)
				return
			}
			err := client.Append(f.objectID, uint64(off), p)
			if err != nil {
				f.fs.dataPool.ReleaseWithError(client)
				errs[idx] = err
				return
			}
			f.fs.dataPool.Release(client)
		}(i, r.Addr)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

// Sync syncs the file to stable storage.
// Implements vfs.File.Sync.
func (f *file) Sync() error {
	if !f.writable {
		return nil
	}

	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return errors.New("file is closed")
	}
	f.mu.Unlock()

	if f.walWriter != nil {
		// WAL writes are already synced by WriteAndSync.
		return nil
	}

	// Sync all replicas.
	var wg sync.WaitGroup
	errs := make([]error, len(f.replicas))

	for i, r := range f.replicas {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()
			client := f.fs.dataPool.Acquire(addr)
			if client == nil {
				errs[idx] = errors.Newf("failed to acquire data client for %s", addr)
				return
			}
			err := client.Sync(f.objectID)
			if err != nil {
				f.fs.dataPool.ReleaseWithError(client)
				errs[idx] = err
				return
			}
			f.fs.dataPool.Release(client)
		}(i, r.Addr)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	f.mu.Lock()
	f.mu.syncedOffset = f.mu.size
	f.mu.Unlock()

	// Update the FS metadata with current size.
	f.fs.updateObjectSize(f.name, f.mu.size)

	return nil
}

// SyncData syncs data (but not necessarily metadata) to stable storage.
// Implements vfs.File.SyncData.
func (f *file) SyncData() error {
	return f.Sync()
}

// SyncTo syncs a prefix of the file to stable storage.
// Implements vfs.File.SyncTo.
func (f *file) SyncTo(length int64) (fullSync bool, err error) {
	// For simplicity, we just do a full sync.
	err = f.Sync()
	return true, err
}

// Close closes the file.
// Implements io.Closer.
func (f *file) Close() error {
	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return nil
	}
	f.mu.closed = true
	size := f.mu.size
	f.mu.Unlock()

	var firstErr error

	// Close the WAL writer if present.
	if f.walWriter != nil {
		if err := f.walWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// For writable files, seal the object via the controller.
	if f.writable {
		ctx := context.Background()
		if err := f.fs.ctrl.Seal(ctx, f.objectID[:], size); err != nil && firstErr == nil {
			firstErr = err
		}

		// Update the final size in metadata.
		f.fs.updateObjectSize(f.name, size)
		f.fs.sealObject(f.name)
	}

	return firstErr
}

// Stat returns file information.
// Implements vfs.File.Stat.
func (f *file) Stat() (vfs.FileInfo, error) {
	f.mu.Lock()
	size := f.mu.size
	f.mu.Unlock()

	return &fileInfo{
		name:  filepath.Base(f.name),
		size:  size,
		isDir: false,
	}, nil
}

// Fd returns the raw file descriptor. Since basaltfs is not backed by
// OS files, this returns InvalidFd.
// Implements vfs.File.Fd.
func (f *file) Fd() uintptr {
	return vfs.InvalidFd
}

// Preallocate is a no-op for basaltfs.
// Implements vfs.File.Preallocate.
func (f *file) Preallocate(offset, length int64) error {
	return nil
}

// Prefetch is a no-op for basaltfs.
// Implements vfs.File.Prefetch.
func (f *file) Prefetch(offset int64, length int64) error {
	return nil
}
