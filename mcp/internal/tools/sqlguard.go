package tools

import (
	"fmt"
	"strings"
)

// QueryOptions controls what the query tools are allowed to do.
type QueryOptions struct {
	// AllowWrites lets execute_query run any statement. When false (the default),
	// only read-only statements are accepted.
	AllowWrites bool

	// OutputDir is the only directory execute_query may write CSV files into.
	// Empty disables file output.
	OutputDir string
}

// readOnlyKeywords are the statement types accepted when writes are disabled.
var readOnlyKeywords = map[string]bool{
	"SELECT":   true,
	"WITH":     true,
	"SHOW":     true,
	"DESCRIBE": true,
	"EXPLAIN":  true,
	"VALUES":   true,
	"TABLE":    true,
}

// checkReadOnly returns an error unless sql is a read-only statement.
//
// Trino runs exactly one statement per request, so checking the leading keyword
// is enough to keep writes out. EXPLAIN ANALYZE is rejected because it executes
// the statement it explains. This is a guardrail for AI clients, not a replacement
// for Trino access control: for example, table functions such as
// system.query() pass SQL to a remote database and are not inspected here.
func checkReadOnly(sql string) error {
	rest := skipNoise(sql)
	kw, rest := nextWord(rest)
	if kw == "" {
		return fmt.Errorf("empty SQL statement")
	}
	if !readOnlyKeywords[kw] {
		return fmt.Errorf("%s statements are blocked: the server is in read-only mode (start it with --allow-writes to permit writes)", kw)
	}
	if kw == "EXPLAIN" {
		next, _ := nextWord(skipNoise(rest))
		if next == "ANALYZE" {
			return fmt.Errorf("EXPLAIN ANALYZE is blocked in read-only mode because it executes the statement")
		}
	}
	return nil
}

// isTrinoSpace matches exactly the whitespace Trino's lexer skips. Anything
// else (e.g. Unicode spaces) is left in place, so the keyword check fails closed.
func isTrinoSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// skipNoise drops leading whitespace, comments, and opening parentheses,
// following Trino's lexer rules: a -- comment ends at \r or \n.
func skipNoise(s string) string {
	for {
		i := 0
		for i < len(s) && isTrinoSpace(s[i]) {
			i++
		}
		s = s[i:]
		switch {
		case strings.HasPrefix(s, "--"):
			if j := strings.IndexAny(s, "\r\n"); j >= 0 {
				s = s[j:]
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			if j := strings.Index(s[2:], "*/"); j >= 0 {
				s = s[j+4:]
			} else {
				return ""
			}
		case strings.HasPrefix(s, "("):
			s = s[1:]
		default:
			return s
		}
	}
}

// nextWord returns the leading run of ASCII letters (upper-cased) and the remainder.
func nextWord(s string) (string, string) {
	i := 0
	for i < len(s) && (s[i] >= 'A' && s[i] <= 'Z' || s[i] >= 'a' && s[i] <= 'z') {
		i++
	}
	return strings.ToUpper(s[:i]), s[i:]
}

// quoteIdent returns s as a quoted Trino identifier, so user input can never
// break out of an identifier position in generated SQL.
func quoteIdent(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("identifier must not be empty")
	}
	if strings.ContainsRune(s, 0) {
		return "", fmt.Errorf("identifier contains a NUL byte")
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`, nil
}

// quoteQualified quotes each dot-separated part of a name such as
// catalog.schema.table. Surrounding quotes on a part are removed first, so
// already-quoted input is not double-quoted.
func quoteQualified(name string, maxParts int) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) > maxParts {
		return "", fmt.Errorf("%q has too many parts (max %d)", name, maxParts)
	}
	quoted := make([]string, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if len(p) >= 2 && strings.HasPrefix(p, `"`) && strings.HasSuffix(p, `"`) {
			p = strings.ReplaceAll(p[1:len(p)-1], `""`, `"`)
		}
		q, err := quoteIdent(p)
		if err != nil {
			return "", fmt.Errorf("invalid name %q: %w", name, err)
		}
		quoted[i] = q
	}
	return strings.Join(quoted, "."), nil
}
