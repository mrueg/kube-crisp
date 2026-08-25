// Package version carries the build version stamped in at link time.
package version

// Version is set by the release build via -ldflags. Development builds report
// the placeholder.
var Version = "dev"
