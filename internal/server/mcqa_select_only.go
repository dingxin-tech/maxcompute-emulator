package server

// A SQLRT session runs whole selects. Anything else — DDL, DML, anything with a side
// effect — the session declines to start, and the client that submitted it runs it
// somewhere else instead.
//
// The order matters, and it is the reason this file exists. A session that *executes* a
// non-select and only then says the result cannot be downloaded is not merely untidy: the
// Java SDK's tunnel read reacts to exactly that answer by re-running the statement offline
// (SQLExecutorImpl#getSessionResultSetByInstanceTunnel -> TunnelRetryStatus.NON_SELECT_QUERY
// -> runQueryInternal(OFFLINE, ..., true)). An INSERT applied that way lands twice.

import (
	"encoding/json"
	"strings"
)

const (
	// mcqaSelectOnlyKey is the setting a client sends to lift the restriction, either in
	// the session's task settings or in the per-statement settings of a sub query.
	mcqaSelectOnlyKey = "odps.sql.session.select.only"
	// mcqaSelectOnlyRefusal is what the service says, and the pair the client acts on:
	// "ODPS-185" is the flag FallbackPolicy#shouldFallback reads to decide that an
	// unsupported-in-session statement should be retried offline. Say only the sentence
	// without the code and the same submission comes back as a hard error instead.
	mcqaSelectOnlyRefusal = "ODPS-1850001 Non select query not supported."
)

// sessionRunsNonSelect reports whether a settings block asks for non-select statements to
// run in the session. Absent or any other value keeps the select-only default.
func sessionRunsNonSelect(settings map[string]string) bool {
	value, ok := settings[mcqaSelectOnlyKey]
	return ok && strings.EqualFold(strings.TrimSpace(value), "false")
}

// mcqaSettingsMap decodes the JSON settings property of a SQLRT task. The same helper
// reads it for the session name, so a settings block that is not a JSON object simply
// yields nothing and keeps the defaults.
func mcqaSettingsMap(settings string) map[string]string {
	if strings.TrimSpace(settings) == "" {
		return nil
	}
	var raw map[string]any
	if e := json.Unmarshal([]byte(settings), &raw); e != nil {
		return nil
	}
	out := map[string]string{}
	for k, v := range raw {
		out[k] = fmtSprint(v)
	}
	return out
}

func fmtSprint(v any) string {
	s, _ := json.Marshal(v)
	return strings.Trim(string(s), `"`)
}

// isSelectStatement is the emulator's stand-in for the planner's own select check: every
// statement in the submission has to start with SELECT or WITH, ignoring comments and the
// parentheses a client may wrap a query in. It is deliberately conservative in the
// direction that costs nothing to be wrong about — anything it does not recognise is run
// offline by the client instead of twice by the session.
func isSelectStatement(sql string) bool {
	statements := sqlStatements(sql)
	if len(statements) == 0 {
		return false
	}
	for _, statement := range statements {
		head := strings.TrimLeft(statement, "( \t\n")
		if !strings.HasPrefix(head, "select") && !strings.HasPrefix(head, "with") {
			return false
		}
	}
	return true
}

// sqlStatements returns the lower-cased text of each statement in a submission with
// comments removed, so that a `select 1; drop table t` cannot pass for a select. Quotes
// are respected: a semicolon inside a string literal does not end a statement.
func sqlStatements(sql string) []string {
	var out []string
	var b strings.Builder
	quote := byte(0)
	for n := 0; n < len(sql); {
		c := sql[n]
		switch {
		case quote != 0 && c == '\\' && quote != '`':
			b.WriteByte(c)
			n++
			if n < len(sql) {
				b.WriteByte(sql[n])
				n++
			}
		case quote != 0:
			if c == quote {
				quote = 0
			}
			b.WriteByte(c)
			n++
		case c == '\'' || c == '"' || c == '`':
			quote = c
			b.WriteByte(c)
			n++
		case c == '-' && n+1 < len(sql) && sql[n+1] == '-':
			for n < len(sql) && sql[n] != '\n' {
				n++
			}
		case c == '/' && n+1 < len(sql) && sql[n+1] == '*':
			end := strings.Index(sql[n+2:], "*/")
			if end < 0 {
				n = len(sql)
			} else {
				n += end + 4
			}
		case c == ';':
			out = addStatement(out, b.String())
			b.Reset()
			n++
		default:
			b.WriteByte(c)
			n++
		}
	}
	return addStatement(out, b.String())
}

func addStatement(out []string, text string) []string {
	if text = strings.TrimSpace(text); text == "" {
		return out
	}
	return append(out, strings.ToLower(text))
}
