module github.com/cockroachdb/basaltfs

go 1.25.3

require (
	github.com/cockroachdb/basaltclient v0.0.0-20260205173632-0028d856e282
	github.com/cockroachdb/errors v1.12.0
	github.com/cockroachdb/pebble v0.0.0-20260121183949-86f48627b1b4
)

require (
	github.com/cockroachdb/crlib v0.0.0-20251122031428-fe658a2dbda1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251029180050-ab9386a59fda // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// Exclude old genproto that conflicts with the new split modules
exclude google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1
