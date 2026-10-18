package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckReadOnly(t *testing.T) {
	allowed := []string{
		"SELECT 1",
		"  select * from t",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"SHOW CATALOGS",
		"DESCRIBE hive.s.t",
		"EXPLAIN SELECT 1",
		"EXPLAIN (TYPE DISTRIBUTED) SELECT 1",
		"VALUES 1, 2",
		"TABLE hive.s.t",
		"-- leading comment\nSELECT 1",
		"-- windows line ending\r\nSELECT 1",
		"\t\r\n SELECT 1",
		"/* block */ SELECT 1",
		"(SELECT 1) UNION (SELECT 2)",
		"EXPLAIN INSERT INTO t VALUES 1", // plain EXPLAIN only plans
	}
	for _, sql := range allowed {
		if err := checkReadOnly(sql); err != nil {
			t.Errorf("checkReadOnly(%q) = %v, want nil", sql, err)
		}
	}

	blocked := []string{
		"INSERT INTO t VALUES 1",
		"DROP TABLE t",
		"delete from t",
		"CREATE TABLE t AS SELECT 1",
		"ALTER TABLE t ADD COLUMN c int",
		"CALL system.sync_partition_metadata('s','t','ADD')",
		"GRANT SELECT ON t TO u",
		"SET SESSION query_max_run_time = '1h'",
		"EXPLAIN ANALYZE DELETE FROM t",
		"explain analyze verbose select 1",
		"/* SELECT */ DROP TABLE t",
		"-- SELECT\nDROP TABLE t",
		"",
		"   ",
		"/* unterminated",
		// Trino ends a -- comment at \r; the guard must too.
		"--\rINSERT INTO t\nSELECT 1",
		"--\rDELETE FROM t WHERE id IN (\nSELECT id FROM t)",
		"EXPLAIN --\rANALYZE\nINSERT INTO t SELECT 1",
		// Whitespace Trino doesn't accept must fail closed.
		"\u0085SELECT 1",
		"\u00a0SELECT 1",
		"\fSELECT 1",
	}
	for _, sql := range blocked {
		if err := checkReadOnly(sql); err == nil {
			t.Errorf("checkReadOnly(%q) = nil, want error", sql)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"hive":          `"hive"`,
		"my-schema":     `"my-schema"`,
		`a"b`:           `"a""b"`,
		"x; DROP TABLE": `"x; DROP TABLE"`,
	}
	for in, want := range cases {
		got, err := quoteIdent(in)
		if err != nil || got != want {
			t.Errorf("quoteIdent(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := quoteIdent(""); err == nil {
		t.Error("quoteIdent(\"\") should fail")
	}
	if _, err := quoteIdent("a\x00b"); err == nil {
		t.Error("quoteIdent with NUL should fail")
	}
}

func TestQuoteQualified(t *testing.T) {
	cases := map[string]string{
		"t":                 `"t"`,
		"s.t":               `"s"."t"`,
		"c.s.t":             `"c"."s"."t"`,
		`"c"."s"."t"`:       `"c"."s"."t"`,
		`c.s.t; DROP TABLE`: `"c"."s"."t; DROP TABLE"`,
	}
	for in, want := range cases {
		got, err := quoteQualified(in, 3)
		if err != nil || got != want {
			t.Errorf("quoteQualified(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"a.b.c.d", "a..b", ""} {
		if _, err := quoteQualified(bad, 3); err == nil {
			t.Errorf("quoteQualified(%q) should fail", bad)
		}
	}
}

func TestResolveOutputPath(t *testing.T) {
	dir := t.TempDir()

	got, err := resolveOutputPath(dir, "out.csv")
	if err != nil || got != filepath.Join(dir, "out.csv") {
		t.Fatalf("plain name: got %q, %v", got, err)
	}
	// Absolute path directly inside the output dir is accepted.
	if _, err := resolveOutputPath(dir, filepath.Join(dir, "abs.csv")); err != nil {
		t.Fatalf("abs path inside dir: %v", err)
	}

	for _, bad := range []string{"../escape.csv", "sub/out.csv", "/etc/passwd", "..", ".", filepath.Join(dir, "sub", "x.csv")} {
		if _, err := resolveOutputPath(dir, bad); err == nil {
			t.Errorf("resolveOutputPath(%q) should fail", bad)
		}
	}

	if _, err := resolveOutputPath("", "out.csv"); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("empty dir should disable output, got %v", err)
	}

	// A symlink planted in the output dir must not be followed.
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link.csv")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if _, err := resolveOutputPath(dir, "link.csv"); err == nil {
		t.Error("symlink should be rejected")
	}
}

func TestOutputDirMustBePrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil { // umask may have reduced it
		t.Fatal(err)
	}
	if _, err := resolveOutputPath(dir, "x.csv"); err == nil || !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("world-writable output dir should be refused, got %v", err)
	}
}

func TestWriteCSVReplacesLinksInsteadOfFollowing(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "out.csv")
	if err := os.Link(victim, path); err != nil {
		t.Skipf("hard links unsupported: %v", err)
	}
	if _, err := writeResultToCSV(&trinoQueryResult{Columns: []string{"a"}, Rows: [][]string{{"1"}}}, path); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "keep" {
		t.Fatalf("hard-linked file was overwritten: %q", b)
	}
	if b, _ := os.ReadFile(path); string(b) != "a\n1\n" {
		t.Fatalf("output wrong: %q", b)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, ".meridian-*")); len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

func TestCacheKeyIncludesMaxRowsAndPrefix(t *testing.T) {
	a := cacheKey("http://c1", "SELECT 1", 10)
	b := cacheKey("http://c1", "SELECT 1", 100)
	if a == b {
		t.Fatal("cache key must differ by maxRows")
	}
	if !strings.HasPrefix(a, coordinatorPrefix("http://c1")) {
		t.Fatal("cache key must start with the coordinator prefix so invalidation works")
	}
}

func TestCacheInvalidatePrefix(t *testing.T) {
	c := newQueryCache(100)
	c.set(cacheKey("http://c1", "SHOW CATALOGS", 0), &trinoQueryResult{}, time.Minute)
	c.set(cacheKey("http://c2", "SHOW CATALOGS", 0), &trinoQueryResult{}, time.Minute)

	c.invalidatePrefix(coordinatorPrefix("http://c1"))

	if _, ok := c.get(cacheKey("http://c1", "SHOW CATALOGS", 0)); ok {
		t.Error("c1 entry should be invalidated")
	}
	if _, ok := c.get(cacheKey("http://c2", "SHOW CATALOGS", 0)); !ok {
		t.Error("c2 entry should survive")
	}
}

func TestCacheBounded(t *testing.T) {
	c := newQueryCache(3)
	for i := 0; i < 10; i++ {
		c.set(cacheKey("http://c", strings.Repeat("x", i), 0), &trinoQueryResult{}, time.Duration(i+1)*time.Minute)
	}
	if n := c.len(); n > 3 {
		t.Fatalf("cache has %d entries, want <= 3", n)
	}
	// The newest (longest-lived) entry must still be there.
	if _, ok := c.get(cacheKey("http://c", strings.Repeat("x", 9), 0)); !ok {
		t.Error("newest entry was evicted")
	}
}
