package tools

import (
	"fmt"
	"os"
	"path/filepath"
)

// resolveOutputPath maps a requested CSV file name to a path inside dir.
//
// Only a bare file name is accepted, so a caller can never write outside dir.
// An absolute path is accepted only when it already points directly into dir,
// which keeps older prompts like "/tmp/meridian/out.csv" working.
func resolveOutputPath(dir, requested string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("output_file is disabled on this server")
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolve output dir: %w", err)
	}

	name := requested
	if filepath.IsAbs(requested) {
		if filepath.Dir(filepath.Clean(requested)) != absDir {
			return "", fmt.Errorf("output_file must be a file name inside %s, got %q", absDir, requested)
		}
		name = filepath.Base(requested)
	}
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return "", fmt.Errorf("output_file must be a plain file name (no directories), got %q", requested)
	}

	if err := os.MkdirAll(absDir, 0o700); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}
	// The directory must be a real directory that other users can't write to;
	// otherwise someone could plant files or links in it.
	di, err := os.Lstat(absDir)
	if err != nil {
		return "", fmt.Errorf("check output dir: %w", err)
	}
	if !di.IsDir() || di.Mode().Perm()&0o022 != 0 {
		return "", fmt.Errorf("output dir %s must be a directory not writable by group or others", absDir)
	}
	path := filepath.Join(absDir, name)

	// Refuse to follow a symlink or overwrite anything that isn't a regular file.
	if fi, err := os.Lstat(path); err == nil && !fi.Mode().IsRegular() {
		return "", fmt.Errorf("output_file %q exists and is not a regular file", name)
	}
	return path, nil
}

// DefaultOutputDir is a per-user directory for CSV exports. It avoids shared
// locations like /tmp where another user could pre-create the directory.
func DefaultOutputDir() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "meridian", "exports")
}
