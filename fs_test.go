package basaltfs

import "testing"

func TestParseServers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		count   int
	}{
		{
			name:    "single server",
			input:   "az1@localhost:26258,localhost:26259",
			wantErr: false,
			count:   1,
		},
		{
			name:    "multiple servers",
			input:   "az1@host1:26258,host1:26259;az2@host2:26258,host2:26259;az3@host3:26258,host3:26259",
			wantErr: false,
			count:   3,
		},
		{
			name:    "with whitespace",
			input:   " az1@host1:26258,host1:26259 ; az2@host2:26258,host2:26259 ",
			wantErr: false,
			count:   2,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
			count:   0,
		},
		{
			name:    "missing @",
			input:   "az1host1:26258,host1:26259",
			wantErr: true,
			count:   0,
		},
		{
			name:    "missing comma",
			input:   "az1@host1:26258host1:26259",
			wantErr: true,
			count:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			servers, err := parseServers(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseServers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(servers) != tt.count {
				t.Errorf("parseServers() got %d servers, want %d", len(servers), tt.count)
			}
		})
	}
}

func TestServerInfoParsing(t *testing.T) {
	input := "az1@ctrl1:26258,data1:26259;az2@ctrl2:26258,data2:26259"
	servers, err := parseServers(input)
	if err != nil {
		t.Fatalf("parseServers() error = %v", err)
	}

	if len(servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(servers))
	}

	// Check first server
	if servers[0].az != "az1" {
		t.Errorf("server[0].az = %q, want %q", servers[0].az, "az1")
	}
	if servers[0].ctrlAddr != "ctrl1:26258" {
		t.Errorf("server[0].ctrlAddr = %q, want %q", servers[0].ctrlAddr, "ctrl1:26258")
	}
	if servers[0].dataAddr != "data1:26259" {
		t.Errorf("server[0].dataAddr = %q, want %q", servers[0].dataAddr, "data1:26259")
	}

	// Check second server
	if servers[1].az != "az2" {
		t.Errorf("server[1].az = %q, want %q", servers[1].az, "az2")
	}
	if servers[1].ctrlAddr != "ctrl2:26258" {
		t.Errorf("server[1].ctrlAddr = %q, want %q", servers[1].ctrlAddr, "ctrl2:26258")
	}
	if servers[1].dataAddr != "data2:26259" {
		t.Errorf("server[1].dataAddr = %q, want %q", servers[1].dataAddr, "data2:26259")
	}
}

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

func TestSelectReplicas(t *testing.T) {
	servers := []serverInfo{
		{az: "az1", ctrlAddr: "ctrl1:26258", dataAddr: "data1:26259"},
		{az: "az2", ctrlAddr: "ctrl2:26258", dataAddr: "data2:26259"},
		{az: "az3", ctrlAddr: "ctrl3:26258", dataAddr: "data3:26259"},
	}

	fs := &FS{
		opts: Options{
			Replication: 2,
			LocalAZ:     "az2",
		},
		servers: servers,
	}

	replicas := fs.selectReplicas()
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}

	// Local AZ should be first
	if replicas[0].az != "az2" {
		t.Errorf("first replica az = %q, want %q (local AZ should be preferred)", replicas[0].az, "az2")
	}
}

func TestSelectReadReplica(t *testing.T) {
	servers := []serverInfo{
		{az: "az1", ctrlAddr: "ctrl1:26258", dataAddr: "data1:26259"},
		{az: "az2", ctrlAddr: "ctrl2:26258", dataAddr: "data2:26259"},
		{az: "az3", ctrlAddr: "ctrl3:26258", dataAddr: "data3:26259"},
	}

	fs := &FS{
		opts: Options{
			LocalAZ: "az2",
		},
		servers: servers,
	}

	replicas := []string{"data1:26259", "data2:26259", "data3:26259"}
	selected := fs.selectReadReplica(replicas)

	// Should prefer local AZ
	if selected != "data2:26259" {
		t.Errorf("selectReadReplica() = %q, want %q (local AZ replica)", selected, "data2:26259")
	}

	// Test with no local replica
	fs.opts.LocalAZ = "az4"
	selected = fs.selectReadReplica(replicas)
	if selected != "data1:26259" {
		t.Errorf("selectReadReplica() = %q, want %q (first replica when no local)", selected, "data1:26259")
	}

	// Test with empty replicas
	selected = fs.selectReadReplica(nil)
	if selected != "" {
		t.Errorf("selectReadReplica(nil) = %q, want empty string", selected)
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

	// ReadAt should return EOF
	n, err = dh.ReadAt(buf, 0)
	if n != 0 {
		t.Errorf("ReadAt() n = %d, want 0", n)
	}

	// Write should return error
	n, err = dh.Write(buf)
	if err == nil {
		t.Error("Write() should return error")
	}

	// WriteAt should return error
	n, err = dh.WriteAt(buf, 0)
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
