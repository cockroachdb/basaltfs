// Package basaltfs provides a Pebble VFS implementation backed by Basalt storage.
//
// BasaltFS implements the pebble/vfs.FS interface, storing all file data as
// objects in Basalt blob servers. It coordinates with the Basalt controller for
// namespace and lifecycle operations (Create, Seal, Delete, Rename, Link) and
// talks directly to blob servers via the custom TCP data protocol for data
// operations (Append, Read, Sync).
//
// Usage:
//
//	fs, err := basaltfs.NewFS(basaltfs.Options{
//	    Controller:  "controller:26257",
//	    DirectoryID: directoryUUID,
//	    LocalZone:   "zone1",
//	})
//	pebbleOpts := &pebble.Options{FS: fs}
package basaltfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/basaltclient"
	"github.com/cockroachdb/basaltclient/basaltpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// Options configures the FS behavior.
type Options struct {
	// Controller is the gRPC address of the Basalt controller.
	Controller string

	// DirectoryID is the resolved store directory ID. The caller (e.g. CRDB)
	// resolves the namespace hierarchy (cluster/store) to a directory ID
	// before constructing the FS.
	DirectoryID basaltpb.UUID

	// LocalZone is this node's zone, used to prefer local replicas for reads.
	LocalZone string
}

// FS implements vfs.FS backed by Basalt storage.
type FS struct {
	opts        Options
	ctrl        *basaltclient.ControllerClient
	dataPool    *basaltclient.BlobDataClientPool
	directoryID basaltpb.UUID

	mu struct {
		sync.Mutex
		// Cache of metadata for known files. Populated from controller
		// Create/List/Stat responses. Keyed by relative path.
		objects map[string]*basaltpb.ObjectMeta
	}
}

var _ vfs.FS = (*FS)(nil)

// NewFS creates a new FS using the given options. It connects to the controller
// and populates the local file cache from the store directory.
func NewFS(opts Options) (*FS, error) {
	if opts.Controller == "" {
		return nil, errors.New("controller address is required")
	}
	var nilUUID basaltpb.UUID
	if opts.DirectoryID == nilUUID {
		return nil, errors.New("directory ID is required")
	}

	ctrl, err := basaltclient.NewControllerClient(opts.Controller)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to controller")
	}

	ctx := context.Background()

	fs := &FS{
		opts:        opts,
		ctrl:        ctrl,
		dataPool:    basaltclient.NewBlobDataClientPool(),
		directoryID: opts.DirectoryID,
	}
	fs.mu.objects = make(map[string]*basaltpb.ObjectMeta)

	// Populate cache from controller.
	entries, err := ctrl.List(ctx, fs.directoryID[:])
	if err != nil {
		ctrl.Close()
		return nil, errors.Wrap(err, "listing store directory")
	}

	for _, entry := range entries {
		if entry.Type == basaltpb.EntryType_ENTRY_TYPE_DIRECTORY {
			continue
		}
		// Get full metadata including replica addresses.
		statResp, err := ctrl.StatByID(ctx, entry.Id[:], false, false)
		if err != nil {
			ctrl.Close()
			return nil, errors.Wrapf(err, "stat object %s (%s)", entry.Name, entry.Id)
		}
		fs.mu.objects[entry.Name] = statResp.Meta
	}

	return fs, nil
}

// Close releases resources associated with the filesystem.
func (fs *FS) Close() error {
	// TODO: Call ctrl.Unmount(mountID) once Mount/Unmount is implemented.
	var firstErr error
	if err := fs.ctrl.Close(); err != nil {
		firstErr = err
	}
	if err := fs.dataPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// isWALFile returns true if the file path indicates a WAL file.
func isWALFile(name string) bool {
	return strings.HasSuffix(name, ".log")
}

// selectReadReplica selects a replica for reading, preferring local zone.
func (fs *FS) selectReadReplica(replicas []basaltpb.ReplicaInfo) string {
	if len(replicas) == 0 {
		return ""
	}
	for _, r := range replicas {
		if r.Zone == fs.opts.LocalZone {
			return r.Addr
		}
	}
	return replicas[0].Addr
}

// splitPath splits a cleaned path into its components.
func splitPath(path string) []string {
	path = filepath.Clean(path)
	if path == "." || path == "/" {
		return nil
	}
	var components []string
	for _, comp := range strings.Split(path, "/") {
		if comp != "" {
			components = append(components, comp)
		}
	}
	return components
}

// splitDirBase splits a path into directory and base components.
func splitDirBase(name string) (dir, base string) {
	dir = filepath.Dir(name)
	base = filepath.Base(name)
	return dir, base
}

// resolveDir walks path components relative to fs.directoryID via StatByPath.
func (fs *FS) resolveDir(ctx context.Context, dir string) (basaltpb.UUID, error) {
	currentID := fs.directoryID
	for _, comp := range splitPath(dir) {
		resp, err := fs.ctrl.StatByPath(ctx, currentID[:], comp, false)
		if err != nil {
			return basaltpb.UUID{}, errors.Wrapf(err, "resolving %q", comp)
		}
		currentID = resp.Meta.Id
	}
	return currentID, nil
}

// resolveDirForPath resolves the parent directory for a given file path,
// returning the parent directory ID and the base name.
func (fs *FS) resolveDirForPath(ctx context.Context, name string) (basaltpb.UUID, string, error) {
	dir, base := splitDirBase(name)
	dirID, err := fs.resolveDir(ctx, dir)
	if err != nil {
		return basaltpb.UUID{}, "", err
	}
	return dirID, base, nil
}

// Create creates a new file for writing.
// Implements vfs.FS.Create.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	name = filepath.Clean(name)
	ctx := context.Background()

	// If file exists, remove it first.
	fs.mu.Lock()
	_, exists := fs.mu.objects[name]
	fs.mu.Unlock()
	if exists {
		if err := fs.Remove(name); err != nil {
			return nil, errors.Wrapf(err, "removing existing file %s", name)
		}
	}

	// Resolve the parent directory.
	dirID, base, err := fs.resolveDirForPath(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(err, "resolving directory for %s", name)
	}

	// Create via controller.
	meta, err := fs.ctrl.Create(ctx, dirID[:], base, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "creating object for %s", name)
	}

	fs.mu.Lock()
	fs.mu.objects[name] = meta
	fs.mu.Unlock()

	f := &file{
		fs:       fs,
		name:     name,
		objectID: basaltclient.ObjectID(meta.Id),
		replicas: meta.Replicas,
		readable: false,
		writable: true,
	}
	f.mu.size = 0

	// For WAL files, use QuorumWriter for efficient quorum writes.
	if isWALFile(name) {
		f.walWriter = basaltclient.NewQuorumWriter(basaltclient.ObjectID(meta.Id), meta.Replicas)
	}

	return f, nil
}

// Open opens a file for reading.
// Implements vfs.FS.Open.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	name = filepath.Clean(name)

	fs.mu.Lock()
	meta, exists := fs.mu.objects[name]
	fs.mu.Unlock()

	if !exists {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}

	f := &file{
		fs:       fs,
		name:     name,
		objectID: basaltclient.ObjectID(meta.Id),
		replicas: meta.Replicas,
		readable: true,
		writable: false,
	}
	f.mu.size = meta.Size_

	for _, opt := range opts {
		opt.Apply(f)
	}

	return f, nil
}

// OpenReadWrite opens a file for reading and writing.
// Implements vfs.FS.OpenReadWrite.
func (fs *FS) OpenReadWrite(
	name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption,
) (vfs.File, error) {
	name = filepath.Clean(name)

	fs.mu.Lock()
	meta, exists := fs.mu.objects[name]
	fs.mu.Unlock()

	if !exists {
		return fs.Create(name, category)
	}

	f := &file{
		fs:       fs,
		name:     name,
		objectID: basaltclient.ObjectID(meta.Id),
		replicas: meta.Replicas,
		readable: true,
		writable: true,
	}
	f.mu.size = meta.Size_

	for _, opt := range opts {
		opt.Apply(f)
	}

	return f, nil
}

// OpenDir opens a directory for syncing.
// Implements vfs.FS.OpenDir.
func (fs *FS) OpenDir(name string) (vfs.File, error) {
	name = filepath.Clean(name)
	return &dirHandle{fs: fs, name: name}, nil
}

// Remove deletes a file.
// Implements vfs.FS.Remove.
func (fs *FS) Remove(name string) error {
	name = filepath.Clean(name)
	ctx := context.Background()

	fs.mu.Lock()
	_, exists := fs.mu.objects[name]
	if exists {
		delete(fs.mu.objects, name)
	}
	fs.mu.Unlock()

	if !exists {
		return nil // Removing non-existent file is a no-op.
	}

	dirID, base, err := fs.resolveDirForPath(ctx, name)
	if err != nil {
		return errors.Wrapf(err, "resolving directory for %s", name)
	}

	_, err = fs.ctrl.Delete(ctx, dirID[:], base)
	if err != nil {
		// Treat NotFound as success — file may have been deleted concurrently.
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// RemoveAll removes a directory and all its contents.
// Implements vfs.FS.RemoveAll.
func (fs *FS) RemoveAll(name string) error {
	name = filepath.Clean(name)
	prefix := name + "/"

	fs.mu.Lock()
	var names []string
	for n := range fs.mu.objects {
		if n == name || strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	for _, n := range names {
		delete(fs.mu.objects, n)
	}
	fs.mu.Unlock()

	ctx := context.Background()
	var firstErr error
	for _, n := range names {
		dirID, base, err := fs.resolveDirForPath(ctx, n)
		if err != nil {
			if firstErr == nil {
				firstErr = errors.Wrapf(err, "resolving directory for %s", n)
			}
			continue
		}
		_, err = fs.ctrl.Delete(ctx, dirID[:], base)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// Rename renames a file.
// Implements vfs.FS.Rename.
func (fs *FS) Rename(oldname, newname string) error {
	oldname = filepath.Clean(oldname)
	newname = filepath.Clean(newname)
	ctx := context.Background()

	// Resolve the parent directory for the old name.
	oldDirID, oldBase, err := fs.resolveDirForPath(ctx, oldname)
	if err != nil {
		return errors.Wrapf(err, "resolving directory for %s", oldname)
	}

	// The controller Rename operates within a single directory, so both
	// old and new names must share the same parent directory.
	_, newBase := splitDirBase(newname)

	if err := fs.ctrl.Rename(ctx, oldDirID[:], oldBase, newBase); err != nil {
		return err
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	meta := fs.mu.objects[oldname]
	delete(fs.mu.objects, oldname)
	delete(fs.mu.objects, newname)
	if meta != nil {
		fs.mu.objects[newname] = meta
	}

	return nil
}

// Link creates a hard link.
// Implements vfs.FS.Link.
func (fs *FS) Link(oldname, newname string) error {
	oldname = filepath.Clean(oldname)
	newname = filepath.Clean(newname)

	fs.mu.Lock()
	meta, exists := fs.mu.objects[oldname]
	fs.mu.Unlock()

	if !exists {
		return &os.PathError{Op: "link", Path: oldname, Err: os.ErrNotExist}
	}

	ctx := context.Background()

	newDirID, newBase, err := fs.resolveDirForPath(ctx, newname)
	if err != nil {
		return errors.Wrapf(err, "resolving directory for %s", newname)
	}

	if err := fs.ctrl.Link(ctx, newDirID[:], newBase, meta.Id[:]); err != nil {
		return err
	}

	// Add new cache entry pointing to same object (shallow copy).
	linkMeta := *meta
	fs.mu.Lock()
	fs.mu.objects[newname] = &linkMeta
	fs.mu.Unlock()

	return nil
}

// ReuseForWrite attempts to reuse an existing file for writing.
// Implements vfs.FS.ReuseForWrite.
func (fs *FS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	// In basaltfs, we don't reuse objects. Delete old and create new.
	if err := fs.Remove(oldname); err != nil {
		return nil, err
	}
	return fs.Create(newname, category)
}

// MkdirAll creates a directory and all parent directories.
// Implements vfs.FS.MkdirAll.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	components := splitPath(dir)
	if len(components) == 0 {
		return nil
	}
	ctx := context.Background()
	currentID := fs.directoryID
	for _, comp := range components {
		newID, err := fs.ctrl.Mkdir(ctx, currentID[:], comp)
		if err != nil {
			// May already exist — look it up.
			resp, lookupErr := fs.ctrl.StatByPath(ctx, currentID[:], comp, false)
			if lookupErr != nil {
				return errors.Wrapf(err, "creating directory %q", comp)
			}
			currentID = resp.Meta.Id
			continue
		}
		currentID = newID
	}
	return nil
}

// Lock creates a lock file.
// Implements vfs.FS.Lock.
func (fs *FS) Lock(name string) (io.Closer, error) {
	// In a distributed system, locking would need coordination.
	// For now, we return a no-op lock since Pebble uses this for
	// preventing concurrent access, which is handled at a higher level.
	return noopCloser{}, nil
}

// List returns the names of files in a directory.
// Implements vfs.FS.List.
func (fs *FS) List(dir string) ([]string, error) {
	ctx := context.Background()
	dirID, err := fs.resolveDir(ctx, dir)
	if err != nil {
		return nil, err
	}
	entries, err := fs.ctrl.List(ctx, dirID[:])
	if err != nil {
		return nil, errors.Wrapf(err, "listing %q", dir)
	}
	result := make([]string, len(entries))
	for i, e := range entries {
		result[i] = e.Name
	}
	sort.Strings(result)
	return result, nil
}

// Stat returns file info.
// Implements vfs.FS.Stat.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	name = filepath.Clean(name)

	// Root directory always exists.
	if name == "." || name == "/" {
		return &fileInfo{name: filepath.Base(name), size: 0, isDir: true}, nil
	}

	// Fast path: check local cache.
	fs.mu.Lock()
	meta, exists := fs.mu.objects[name]
	fs.mu.Unlock()
	if exists {
		return &fileInfo{
			name: filepath.Base(name), size: meta.Size_, isDir: false,
		}, nil
	}

	// Slow path: ask the controller.
	ctx := context.Background()
	dir, base := splitDirBase(name)
	dirID, err := fs.resolveDir(ctx, dir)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}
	resp, err := fs.ctrl.StatByPath(ctx, dirID[:], base, false)
	if err != nil {
		if isNotFound(err) {
			return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
		}
		return nil, err
	}
	isDir := resp.Type == basaltpb.EntryType_ENTRY_TYPE_DIRECTORY
	size := int64(0)
	if resp.Meta != nil && !isDir {
		size = resp.Meta.Size_
	}
	return &fileInfo{
		name: filepath.Base(name), size: size, isDir: isDir,
	}, nil
}

// PathBase returns the last element of path.
// Implements vfs.FS.PathBase.
func (fs *FS) PathBase(path string) string {
	return filepath.Base(path)
}

// PathJoin joins path elements.
// Implements vfs.FS.PathJoin.
func (fs *FS) PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

// PathDir returns all but the last element of path.
// Implements vfs.FS.PathDir.
func (fs *FS) PathDir(path string) string {
	return filepath.Dir(path)
}

// GetDiskUsage returns disk space statistics.
// Implements vfs.FS.GetDiskUsage.
func (fs *FS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var used uint64
	for _, meta := range fs.mu.objects {
		used += uint64(meta.Size_)
	}

	// Return a large available space since we're backed by distributed storage.
	return vfs.DiskUsage{
		AvailBytes: 1 << 60, // 1 EiB
		TotalBytes: 1 << 60,
		UsedBytes:  used,
	}, nil
}

// Unwrap returns nil as basaltfs is not a wrapping filesystem.
// Implements vfs.FS.Unwrap.
func (fs *FS) Unwrap() vfs.FS {
	return nil
}

// updateObjectSize updates the size of an object in the metadata.
func (fs *FS) updateObjectSize(name string, size int64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if meta, exists := fs.mu.objects[name]; exists {
		meta.Size_ = size
	}
}

// sealObject marks an object as sealed in the metadata.
func (fs *FS) sealObject(name string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if meta, exists := fs.mu.objects[name]; exists {
		if meta.SealedAtNanos == 0 {
			meta.SealedAtNanos = 1 // Non-zero means sealed.
		}
	}
}

// isNotFound returns true if the error is a gRPC NotFound status.
func isNotFound(err error) bool {
	return grpcstatus.Code(err) == codes.NotFound
}
