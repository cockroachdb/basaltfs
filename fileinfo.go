package basaltfs

import (
	"io/fs"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// fileInfo implements vfs.FileInfo for basaltfs files and directories.
type fileInfo struct {
	name  string
	size  int64
	isDir bool
}

var _ vfs.FileInfo = (*fileInfo)(nil)

// Name returns the base name of the file.
func (fi *fileInfo) Name() string {
	return fi.name
}

// Size returns the file size in bytes.
func (fi *fileInfo) Size() int64 {
	return fi.size
}

// Mode returns the file mode bits.
func (fi *fileInfo) Mode() fs.FileMode {
	if fi.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}

// ModTime returns the modification time. Since basaltfs doesn't track
// modification times, it returns a zero time.
func (fi *fileInfo) ModTime() time.Time {
	return time.Time{}
}

// IsDir reports whether the file describes a directory.
func (fi *fileInfo) IsDir() bool {
	return fi.isDir
}

// Sys returns nil as there is no underlying system-specific data.
func (fi *fileInfo) Sys() any {
	return nil
}

// DeviceID returns a zero DeviceID since basaltfs is not backed by a
// physical device.
func (fi *fileInfo) DeviceID() vfs.DeviceID {
	return vfs.DeviceID{}
}
