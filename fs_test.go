package basaltfs

import (
	"io"
	"testing"

	"github.com/cockroachdb/basaltclient/basaltpb"
)

func TestIsWALFile(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"000001.log", true},
		{"wal/000001.log", true},
		{"data.sst", false},
		{"MANIFEST-000001", false},
		{"OPTIONS-000001", false},
		{"CURRENT", false},
		{".log", true},
		{"log", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isWALFile(tt.name); got != tt.want {
				t.Errorf("isWALFile(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestFileInfo(t *testing.T) {
	fi := &fileInfo{
		name:  "test.sst",
		size:  1234,
		isDir: false,
	}

	if fi.Name() != "test.sst" {
		t.Errorf("Name() = %q, want %q", fi.Name(), "test.sst")
	}
	if fi.Size() != 1234 {
		t.Errorf("Size() = %d, want %d", fi.Size(), 1234)
	}
	if fi.IsDir() {
		t.Error("IsDir() = true, want false")
	}
	if fi.Mode().IsDir() {
		t.Error("Mode().IsDir() = true, want false")
	}

	// Test directory
	di := &fileInfo{
		name:  "subdir",
		size:  0,
		isDir: true,
	}

	if !di.IsDir() {
		t.Error("IsDir() = false, want true")
	}
	if !di.Mode().IsDir() {
		t.Error("Mode().IsDir() = false, want true")
	}
}

func TestSelectReadReplica(t *testing.T) {
	fs := &FS{
		opts: Options{
			LocalZone: "zone2",
		},
	}

	replicas := []basaltpb.ReplicaInfo{
		{Addr: "data1:26259", Zone: "zone1"},
		{Addr: "data2:26259", Zone: "zone2"},
		{Addr: "data3:26259", Zone: "zone3"},
	}
	selected := fs.selectReadReplica(replicas)

	// Should prefer local zone.
	if selected != "data2:26259" {
		t.Errorf("selectReadReplica() = %q, want %q (local zone replica)", selected, "data2:26259")
	}

	// Test with no local replica.
	fs.opts.LocalZone = "zone4"
	selected = fs.selectReadReplica(replicas)
	if selected != "data1:26259" {
		t.Errorf("selectReadReplica() = %q, want %q (first replica when no local)", selected, "data1:26259")
	}

	// Test with empty replicas.
	selected = fs.selectReadReplica(nil)
	if selected != "" {
		t.Errorf("selectReadReplica(nil) = %q, want empty string", selected)
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path string
		want []string
	}{
		{".", nil},
		{"/", nil},
		{"", nil},
		{"foo", []string{"foo"}},
		{"foo/bar", []string{"foo", "bar"}},
		{"foo/bar/baz", []string{"foo", "bar", "baz"}},
		{"/foo/bar", []string{"foo", "bar"}},
		{"foo//bar", []string{"foo", "bar"}},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := splitPath(tt.path)
			if len(got) != len(tt.want) {
				t.Errorf("splitPath(%q) = %v, want %v", tt.path, got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitPath(%q)[%d] = %q, want %q", tt.path, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSplitDirBase(t *testing.T) {
	tests := []struct {
		name     string
		wantDir  string
		wantBase string
	}{
		{"foo.sst", ".", "foo.sst"},
		{"wal/000001.log", "wal", "000001.log"},
		{"a/b/c.txt", "a/b", "c.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, base := splitDirBase(tt.name)
			if dir != tt.wantDir {
				t.Errorf("splitDirBase(%q) dir = %q, want %q", tt.name, dir, tt.wantDir)
			}
			if base != tt.wantBase {
				t.Errorf("splitDirBase(%q) base = %q, want %q", tt.name, base, tt.wantBase)
			}
		})
	}
}

func TestNoopCloser(t *testing.T) {
	nc := noopCloser{}
	if err := nc.Close(); err != nil {
		t.Errorf("noopCloser.Close() = %v, want nil", err)
	}
}

func TestDirHandleMethods(t *testing.T) {
	dh := &dirHandle{name: "/test/dir"}

	// Read should return EOF
	buf := make([]byte, 10)
	n, err := dh.Read(buf)
	if n != 0 {
		t.Errorf("Read() n = %d, want 0", n)
	}
	if err != io.EOF {
		t.Errorf("Read() err = %v, want io.EOF", err)
	}

	// ReadAt should return EOF
	n, err = dh.ReadAt(buf, 0)
	if n != 0 {
		t.Errorf("ReadAt() n = %d, want 0", n)
	}
	if err != io.EOF {
		t.Errorf("ReadAt() err = %v, want io.EOF", err)
	}

	// Write should return error
	n, err = dh.Write(buf)
	if n != 0 {
		t.Errorf("Write() n = %d, want 0", n)
	}
	if err == nil {
		t.Error("Write() should return error")
	}

	// WriteAt should return error
	n, err = dh.WriteAt(buf, 0)
	if n != 0 {
		t.Errorf("WriteAt() n = %d, want 0", n)
	}
	if err == nil {
		t.Error("WriteAt() should return error")
	}

	// Sync should succeed
	if err := dh.Sync(); err != nil {
		t.Errorf("Sync() = %v, want nil", err)
	}

	// SyncData should succeed
	if err := dh.SyncData(); err != nil {
		t.Errorf("SyncData() = %v, want nil", err)
	}

	// SyncTo should succeed
	fullSync, err := dh.SyncTo(100)
	if err != nil {
		t.Errorf("SyncTo() error = %v, want nil", err)
	}
	if fullSync {
		t.Error("SyncTo() fullSync = true, want false")
	}

	// Close should succeed
	if err := dh.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}

	// Stat should return directory info
	fi, err := dh.Stat()
	if err != nil {
		t.Errorf("Stat() error = %v, want nil", err)
	}
	if !fi.IsDir() {
		t.Error("Stat().IsDir() = false, want true")
	}
	if fi.Name() != "dir" {
		t.Errorf("Stat().Name() = %q, want %q", fi.Name(), "dir")
	}

	// Fd should return InvalidFd
	if dh.Fd() == 0 {
		t.Error("Fd() = 0, want InvalidFd")
	}

	// Preallocate should succeed (no-op)
	if err := dh.Preallocate(0, 100); err != nil {
		t.Errorf("Preallocate() = %v, want nil", err)
	}

	// Prefetch should succeed (no-op)
	if err := dh.Prefetch(0, 100); err != nil {
		t.Errorf("Prefetch() = %v, want nil", err)
	}
}
