// Package basaltfs provides a Pebble VFS implementation backed by Basalt storage.
//
// BasaltFS implements the pebble/vfs.FS interface, storing all file data as
// objects in Basalt blob servers. It coordinates with blob servers directly
// for object creation, data operations, and sealing.
//
// Usage:
//
//	fs, err := basaltfs.NewFS(basaltfs.Options{
//	    Servers: "zone1@ctrl1:26258,data1:26259;zone2@ctrl2:26258,data2:26259",
//	    LocalZone: "zone1",
//	})
//	pebbleOpts := &pebble.Options{FS: fs}
package basaltfs

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/basaltclient"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// Options configures the FS behavior.
type Options struct {
	// Replication is the number of replicas for each object.
	// Default: 3
	Replication int

	// LocalZone is this node's zone, used to prefer local replicas
	// for reads.
	LocalZone string

	// Servers specifies the blob servers to use. Format:
	// "zone@ctrlAddr,dataAddr;zone@ctrlAddr,dataAddr;..."
	// Example: "zone1@localhost:26258,localhost:26259;zone2@localhost:26260,localhost:26261"
	Servers string
}

// serverInfo holds connection information for a single blob server.
type serverInfo struct {
	zone     string // zone
	ctrlAddr string // gRPC control address (Create, Seal, Delete, Stat)
	dataAddr string // TCP data address (Append, Read)
}

// objectMeta holds metadata about a stored object.
type objectMeta struct {
	objectID basaltclient.ObjectID
	replicas []string // data addresses of replicas
	size     int64
	sealed   bool
}

// FS implements vfs.FS backed by Basalt storage.
type FS struct {
	opts     Options
	servers  []serverInfo
	dataPool *basaltclient.BlobDataClientPool

	mu struct {
		sync.Mutex
		controlClients map[string]*basaltclient.BlobControlClient // keyed by ctrlAddr
		objects        map[string]*objectMeta                     // keyed by file path
		dirs           map[string]struct{}                        // virtual directories
	}
}

var _ vfs.FS = (*FS)(nil)

// NewFS creates a new FS using the given options.
func NewFS(opts Options) (*FS, error) {
	if opts.Replication == 0 {
		opts.Replication = 3
	}

	servers, err := parseServers(opts.Servers)
	if err != nil {
		return nil, err
	}

	if len(servers) < opts.Replication {
		return nil, errors.Newf("insufficient servers: have %d, need %d for replication",
			len(servers), opts.Replication)
	}

	fs := &FS{
		opts:     opts,
		servers:  servers,
		dataPool: basaltclient.NewBlobDataClientPool(),
	}
	fs.mu.controlClients = make(map[string]*basaltclient.BlobControlClient)
	fs.mu.objects = make(map[string]*objectMeta)
	fs.mu.dirs = make(map[string]struct{})

	// Create root directory.
	fs.mu.dirs["/"] = struct{}{}

	return fs, nil
}

// parseServers parses the server configuration string.
// Format: "zone@ctrlAddr,dataAddr;zone@ctrlAddr,dataAddr;..."
func parseServers(s string) ([]serverInfo, error) {
	if s == "" {
		return nil, errors.New("no servers specified")
	}

	var servers []serverInfo
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Parse "zone@ctrlAddr,dataAddr"
		atIdx := strings.Index(part, "@")
		if atIdx == -1 {
			return nil, errors.Newf("invalid server format %q: missing '@'", part)
		}
		zone := part[:atIdx]
		addrs := part[atIdx+1:]

		commaIdx := strings.Index(addrs, ",")
		if commaIdx == -1 {
			return nil, errors.Newf("invalid server format %q: missing ','", part)
		}
		ctrlAddr := addrs[:commaIdx]
		dataAddr := addrs[commaIdx+1:]

		servers = append(servers, serverInfo{
			zone:     zone,
			ctrlAddr: ctrlAddr,
			dataAddr: dataAddr,
		})
	}

	if len(servers) == 0 {
		return nil, errors.New("no servers parsed from configuration")
	}

	return servers, nil
}

// Close releases resources associated with the filesystem.
func (fs *FS) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var firstErr error
	for _, client := range fs.mu.controlClients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	fs.mu.controlClients = nil

	if err := fs.dataPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// getControlClient returns a cached control client for the given address,
// creating one if necessary.
func (fs *FS) getControlClient(ctrlAddr string) (*basaltclient.BlobControlClient, error) {
	fs.mu.Lock()
	client := fs.mu.controlClients[ctrlAddr]
	fs.mu.Unlock()

	if client != nil {
		return client, nil
	}

	// Create new client.
	newClient, err := basaltclient.NewBlobControlClient(ctrlAddr)
	if err != nil {
		return nil, err
	}

	fs.mu.Lock()
	// Check again in case another goroutine created it.
	if existing := fs.mu.controlClients[ctrlAddr]; existing != nil {
		fs.mu.Unlock()
		_ = newClient.Close()
		return existing, nil
	}
	fs.mu.controlClients[ctrlAddr] = newClient
	fs.mu.Unlock()

	return newClient, nil
}

// generateObjectID generates a new random object ID.
func generateObjectID() (basaltclient.ObjectID, error) {
	var id basaltclient.ObjectID
	if _, err := rand.Read(id[:]); err != nil {
		return id, errors.Wrap(err, "generating object ID")
	}
	return id, nil
}

// selectReplicas selects servers for replication, preferring the local zone.
func (fs *FS) selectReplicas() []serverInfo {
	n := fs.opts.Replication
	if n > len(fs.servers) {
		n = len(fs.servers)
	}

	// Sort servers with local zone first.
	sorted := make([]serverInfo, len(fs.servers))
	copy(sorted, fs.servers)
	sort.SliceStable(sorted, func(i, j int) bool {
		iLocal := sorted[i].zone == fs.opts.LocalZone
		jLocal := sorted[j].zone == fs.opts.LocalZone
		return iLocal && !jLocal
	})

	return sorted[:n]
}

// selectReadReplica selects a replica for reading, preferring local zone.
func (fs *FS) selectReadReplica(replicas []string) string {
	if len(replicas) == 0 {
		return ""
	}

	// Try to find a local replica.
	for _, r := range replicas {
		for _, s := range fs.servers {
			if s.dataAddr == r && s.zone == fs.opts.LocalZone {
				return r
			}
		}
	}

	// Fall back to first replica.
	return replicas[0]
}

// createOnAll creates an object on all specified replicas.
func (fs *FS) createOnAll(
	ctx context.Context, id basaltclient.ObjectID, replicas []serverInfo,
) error {
	var wg sync.WaitGroup
	errs := make([]error, len(replicas))

	for i, r := range replicas {
		wg.Add(1)
		go func(idx int, replica serverInfo) {
			defer wg.Done()
			client, err := fs.getControlClient(replica.ctrlAddr)
			if err != nil {
				errs[idx] = err
				return
			}
			errs[idx] = client.Create(ctx, id)
		}(i, r)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// sealOnAll seals an object on all specified replicas.
func (fs *FS) sealOnAll(
	ctx context.Context, id basaltclient.ObjectID, replicas []serverInfo,
) (int64, error) {
	var wg sync.WaitGroup
	sizes := make([]int64, len(replicas))
	errs := make([]error, len(replicas))

	for i, r := range replicas {
		wg.Add(1)
		go func(idx int, replica serverInfo) {
			defer wg.Done()
			client, err := fs.getControlClient(replica.ctrlAddr)
			if err != nil {
				errs[idx] = err
				return
			}
			sizes[idx], errs[idx] = client.Seal(ctx, id)
		}(i, r)
	}

	wg.Wait()

	var maxSize int64
	for i, err := range errs {
		if err != nil {
			return 0, err
		}
		if sizes[i] > maxSize {
			maxSize = sizes[i]
		}
	}
	return maxSize, nil
}

// deleteOnAll deletes an object from all specified replicas.
func (fs *FS) deleteOnAll(
	ctx context.Context, id basaltclient.ObjectID, replicas []serverInfo,
) error {
	var wg sync.WaitGroup
	errs := make([]error, len(replicas))

	for i, r := range replicas {
		wg.Add(1)
		go func(idx int, replica serverInfo) {
			defer wg.Done()
			client, err := fs.getControlClient(replica.ctrlAddr)
			if err != nil {
				errs[idx] = err
				return
			}
			errs[idx] = client.Delete(ctx, id)
		}(i, r)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// replicasForDataAddrs returns the serverInfo for the given data addresses.
func (fs *FS) replicasForDataAddrs(dataAddrs []string) []serverInfo {
	var result []serverInfo
	for _, addr := range dataAddrs {
		for _, s := range fs.servers {
			if s.dataAddr == addr {
				result = append(result, s)
				break
			}
		}
	}
	return result
}

// isWALFile returns true if the file path indicates a WAL file.
func isWALFile(name string) bool {
	return strings.HasSuffix(name, ".log")
}

// Create creates a new file for writing.
// Implements vfs.FS.Create.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	name = filepath.Clean(name)

	// Generate object ID.
	id, err := generateObjectID()
	if err != nil {
		return nil, err
	}

	// Select replicas.
	replicas := fs.selectReplicas()
	dataAddrs := make([]string, len(replicas))
	for i, r := range replicas {
		dataAddrs[i] = r.dataAddr
	}

	// Create object on all replicas.
	ctx := context.Background()
	if err := fs.createOnAll(ctx, id, replicas); err != nil {
		return nil, errors.Wrapf(err, "creating object for %s", name)
	}

	// Store metadata.
	meta := &objectMeta{
		objectID: id,
		replicas: dataAddrs,
		size:     0,
		sealed:   false,
	}

	fs.mu.Lock()
	// Remove any existing object with this name.
	if old, exists := fs.mu.objects[name]; exists {
		fs.mu.Unlock()
		// Delete the old object.
		oldReplicas := fs.replicasForDataAddrs(old.replicas)
		_ = fs.deleteOnAll(ctx, old.objectID, oldReplicas)
		fs.mu.Lock()
	}
	fs.mu.objects[name] = meta
	fs.mu.Unlock()

	// Create file handle.
	f := &file{
		fs:       fs,
		name:     name,
		objectID: id,
		replicas: dataAddrs,
		readable: false,
		writable: true,
	}
	f.mu.size = 0

	// For WAL files, use QuorumWriter for efficient quorum writes.
	if isWALFile(name) {
		f.walWriter = basaltclient.NewQuorumWriter(id, dataAddrs)
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
		objectID: meta.objectID,
		replicas: meta.replicas,
		readable: true,
		writable: false,
	}
	f.mu.size = meta.size

	// Apply open options.
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
		// Create a new file.
		return fs.Create(name, category)
	}

	f := &file{
		fs:       fs,
		name:     name,
		objectID: meta.objectID,
		replicas: meta.replicas,
		readable: true,
		writable: true,
	}
	f.mu.size = meta.size

	// Apply open options.
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

	fs.mu.Lock()
	meta, exists := fs.mu.objects[name]
	if exists {
		delete(fs.mu.objects, name)
	}
	fs.mu.Unlock()

	if !exists {
		return nil // Removing non-existent file is a no-op.
	}

	// Delete from all replicas.
	ctx := context.Background()
	replicas := fs.replicasForDataAddrs(meta.replicas)
	return fs.deleteOnAll(ctx, meta.objectID, replicas)
}

// RemoveAll removes a directory and all its contents.
// Implements vfs.FS.RemoveAll.
func (fs *FS) RemoveAll(name string) error {
	name = filepath.Clean(name)
	prefix := name + "/"

	fs.mu.Lock()
	// Collect all objects to delete.
	var toDelete []*objectMeta
	var names []string
	for n, meta := range fs.mu.objects {
		if n == name || strings.HasPrefix(n, prefix) {
			toDelete = append(toDelete, meta)
			names = append(names, n)
		}
	}
	// Remove from map.
	for _, n := range names {
		delete(fs.mu.objects, n)
	}
	// Remove directory entries.
	delete(fs.mu.dirs, name)
	for dir := range fs.mu.dirs {
		if strings.HasPrefix(dir, prefix) {
			delete(fs.mu.dirs, dir)
		}
	}
	fs.mu.Unlock()

	// Delete objects from replicas.
	ctx := context.Background()
	var firstErr error
	for _, meta := range toDelete {
		replicas := fs.replicasForDataAddrs(meta.replicas)
		if err := fs.deleteOnAll(ctx, meta.objectID, replicas); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Rename renames a file.
// Implements vfs.FS.Rename.
func (fs *FS) Rename(oldname, newname string) error {
	oldname = filepath.Clean(oldname)
	newname = filepath.Clean(newname)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	meta, exists := fs.mu.objects[oldname]
	if !exists {
		return &os.PathError{Op: "rename", Path: oldname, Err: os.ErrNotExist}
	}

	// Remove any existing file at newname.
	delete(fs.mu.objects, newname)

	// Move metadata to new name.
	delete(fs.mu.objects, oldname)
	fs.mu.objects[newname] = meta

	return nil
}

// Link creates a hard link.
// Implements vfs.FS.Link.
func (fs *FS) Link(oldname, newname string) error {
	oldname = filepath.Clean(oldname)
	newname = filepath.Clean(newname)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	meta, exists := fs.mu.objects[oldname]
	if !exists {
		return &os.PathError{Op: "link", Path: oldname, Err: os.ErrNotExist}
	}

	// Create a copy of the metadata pointing to the same object.
	// Note: This doesn't implement true hard link semantics (ref counting).
	linkMeta := &objectMeta{
		objectID: meta.objectID,
		replicas: meta.replicas,
		size:     meta.size,
		sealed:   meta.sealed,
	}
	fs.mu.objects[newname] = linkMeta

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
	dir = filepath.Clean(dir)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Add all path components as directories.
	current := "/"
	for _, part := range strings.Split(dir, "/") {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		fs.mu.dirs[current] = struct{}{}
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
	dir = filepath.Clean(dir)
	if dir != "/" && !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	if dir == "/" {
		dir = ""
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	seen := make(map[string]struct{})

	// List objects.
	for name := range fs.mu.objects {
		if dir == "" || strings.HasPrefix(name, dir) {
			// Get the relative name.
			rel := strings.TrimPrefix(name, dir)
			rel = strings.TrimPrefix(rel, "/")
			// Get just the first component (file or directory name).
			if idx := strings.Index(rel, "/"); idx != -1 {
				rel = rel[:idx]
			}
			if rel != "" {
				seen[rel] = struct{}{}
			}
		}
	}

	// List subdirectories.
	for d := range fs.mu.dirs {
		if dir == "" || strings.HasPrefix(d, dir) {
			rel := strings.TrimPrefix(d, dir)
			rel = strings.TrimPrefix(rel, "/")
			if idx := strings.Index(rel, "/"); idx != -1 {
				rel = rel[:idx]
			}
			if rel != "" {
				seen[rel] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	sort.Strings(result)

	return result, nil
}

// Stat returns file info.
// Implements vfs.FS.Stat.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	name = filepath.Clean(name)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check if it's an object.
	if meta, exists := fs.mu.objects[name]; exists {
		return &fileInfo{
			name:  filepath.Base(name),
			size:  meta.size,
			isDir: false,
		}, nil
	}

	// Check if it's a directory.
	if _, exists := fs.mu.dirs[name]; exists {
		return &fileInfo{
			name:  filepath.Base(name),
			size:  0,
			isDir: true,
		}, nil
	}

	// Check if any object has this as a prefix (implicit directory).
	prefix := name + "/"
	for n := range fs.mu.objects {
		if strings.HasPrefix(n, prefix) {
			return &fileInfo{
				name:  filepath.Base(name),
				size:  0,
				isDir: true,
			}, nil
		}
	}

	return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
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
		used += uint64(meta.size)
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
		meta.size = size
	}
}

// sealObject marks an object as sealed in the metadata.
func (fs *FS) sealObject(name string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if meta, exists := fs.mu.objects[name]; exists {
		meta.sealed = true
	}
}

// backgroundContext returns a context for background operations.
func (fs *FS) backgroundContext() context.Context {
	return context.Background()
}
