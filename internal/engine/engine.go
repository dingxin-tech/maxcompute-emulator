package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

type Engine struct {
	mu      sync.Mutex
	db      *sql.DB
	maxRows int
}

func Open(path string, maxRows int) (*Engine, error) {
	db, e := sql.Open("duckdb", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{"SET memory_limit='512MB'", "SET threads=2", "SET enable_external_access=false", "CREATE TABLE IF NOT EXISTS main.emulator_projects(name VARCHAR PRIMARY KEY)", "CREATE TABLE IF NOT EXISTS main.emulator_catalog(namespace VARCHAR, name VARCHAR, definition VARCHAR, PRIMARY KEY(namespace,name))", "CREATE TABLE IF NOT EXISTS main.emulator_resources(namespace VARCHAR, name VARCHAR, size BIGINT, definition VARCHAR, content BLOB, PRIMARY KEY(namespace,name))", "CREATE TABLE IF NOT EXISTS main.emulator_functions(namespace VARCHAR, name VARCHAR, definition VARCHAR, PRIMARY KEY(namespace,name))"} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	if e = migrateTableIDs(db); e != nil {
		db.Close()
		return nil, e
	}
	return &Engine{db: db, maxRows: maxRows}, nil
}
func (e *Engine) Close() error { return e.db.Close() }
func namespace(p, s string) string {
	x := sha256.Sum256([]byte(p + "\x00" + s))
	return "n_" + hex.EncodeToString(x[:12])
}
func (e *Engine) transaction(ctx context.Context, p, s string) (*sql.Tx, string, error) {
	ns := namespace(p, s)
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ns, err
	}
	if _, err = tx.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+Quote(ns)); err == nil {
		_, err = tx.ExecContext(ctx, "SET schema = '"+ns+"'")
	}
	if err != nil {
		tx.Rollback()
		return nil, ns, err
	}
	return tx, ns, nil
}
func load(ctx context.Context, tx *sql.Tx, ns, name string) (Table, error) {
	var raw string
	t := Table{}
	err := tx.QueryRowContext(ctx, "SELECT definition FROM main.emulator_catalog WHERE namespace=? AND name=?", ns, word(name)).Scan(&raw)
	if err != nil {
		return t, fmt.Errorf("NoSuchTable: %s", name)
	}
	if err = json.Unmarshal([]byte(raw), &t); err != nil {
		return t, err
	}
	for i := range t.Columns {
		t.Columns[i].Parsed, err = ParseType(t.Columns[i].Type)
		if err != nil {
			return t, err
		}
	}
	for i := range t.Partitions {
		t.Partitions[i].Parsed, err = ParseType(t.Partitions[i].Type)
		if err != nil {
			return t, err
		}
	}
	return t, nil
}
func (e *Engine) Table(ctx context.Context, p, s, n string) (Table, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, ns, err := e.transaction(ctx, p, s)
	if err != nil {
		return Table{}, err
	}
	defer tx.Rollback()
	t, err := load(ctx, tx, ns, n)
	return t, err
}
func (e *Engine) Tables(ctx context.Context, p, s string) ([]Table, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rows, err := e.db.QueryContext(ctx, "SELECT definition FROM main.emulator_catalog WHERE namespace=? ORDER BY name", namespace(p, s))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Table{}
	for rows.Next() {
		var b string
		var t Table
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(b), &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (e *Engine) Execute(ctx context.Context, p, s, sqlText string) (Result, error) {
	stmts, err := Lex(sqlText)
	if err != nil {
		return Result{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, ns, err := e.transaction(ctx, p, s)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	var result Result
	for _, ts := range stmts {
		ts = localQualifiedNames(ts, p, s)
		result, err = e.exec(ctx, tx, ns, ts)
		if err != nil {
			return Result{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO main.emulator_projects VALUES (?) ON CONFLICT DO NOTHING", p); err != nil {
		return Result{}, err
	}
	return result, tx.Commit()
}
func (e *Engine) exec(ctx context.Context, tx *sql.Tx, ns string, ts []string) (Result, error) {
	none := Result{}
	if len(ts) == 0 {
		return none, nil
	}
	for _, token := range ts {
		w := word(token)
		if w == "main" || w == "emulator_catalog" || w == "emulator_projects" || w == "information_schema" || w == "pg_catalog" || strings.HasPrefix(w, "n_") || strings.HasPrefix(w, "duckdb_") || strings.HasPrefix(w, "pragma_") || w == "query" || w == "query_table" {
			return none, fmt.Errorf("UnsupportedFeature: reserved database namespace or function")
		}
		switch w {
		case "read_csv", "read_csv_auto", "read_parquet", "parquet_scan", "sqlite_scan", "postgres_scan", "httpfs", "load", "install", "attach", "detach", "copy", "pragma", "export", "import":
			return none, fmt.Errorf("UnsupportedFeature: external access is disabled")
		}
	}
	switch word(ts[0]) {
	case "set":
		if len(ts) >= 3 && strings.ToLower(strings.Join(ts[1:], "")) == "odps.sql.type.system.odps2=true" {
			return none, nil
		}
		return none, fmt.Errorf("UnsupportedFeature: SET option")
	case "alter":
		return none, e.alterMetadata(ctx, tx, ns, ts)
	case "create":
		props := map[string]string{}
		var primaryKeys []string
		var ddlErr error
		ts, primaryKeys, ddlErr = extractPrimaryKey(ts)
		if ddlErr != nil {
			return none, ddlErr
		}
		if word(ts[1]) == "function" {
			return none, fmt.Errorf("UnsupportedFeature: CREATE FUNCTION; register UDFs through the /registration/functions REST API")
		}
		if len(ts) < 4 || word(ts[1]) != "table" {
			return none, fmt.Errorf("UnsupportedFeature: CREATE")
		}
		pos := 2
		exists := false
		if len(ts) > 5 && word(ts[pos]) == "if" {
			exists = true
			pos += 3
		}
		name, pos, err := nameAt(ts, pos)
		if err != nil {
			return none, err
		}
		cols, pos, err := parseColumns(ts, pos)
		if err != nil {
			return none, err
		}
		parts := []Column{}
		if pos < len(ts) && word(ts[pos]) == "partitioned" {
			if pos+1 >= len(ts) || word(ts[pos+1]) != "by" {
				return none, fmt.Errorf("expected PARTITIONED BY")
			}
			parts, pos, err = parseColumns(ts, pos+2)
			if err != nil {
				return none, err
			}
		}
		for _, c := range parts {
			if c.Parsed.Name != "string" {
				return none, fmt.Errorf("UnsupportedFeature: partition columns must be STRING")
			}
		}
		if pos != len(ts) {
			for pos < len(ts) {
				switch word(ts[pos]) {
				case "stored":
					if pos+2 >= len(ts) || word(ts[pos+1]) != "as" || word(ts[pos+2]) != "aliorc" {
						return none, fmt.Errorf("unsupported storage format")
					}
					pos += 3
				case "comment":
					if pos+1 >= len(ts) {
						return none, fmt.Errorf("missing comment")
					}
					props["comment"] = strings.Trim(ts[pos+1], "'")
					pos += 2
				case "lifecycle":
					if pos+1 >= len(ts) {
						return none, fmt.Errorf("missing lifecycle")
					}
					props["lifecycle"] = ts[pos+1]
					pos += 2
				case "tblproperties":
					pos++
					if pos >= len(ts) || ts[pos] != "(" {
						return none, fmt.Errorf("invalid properties")
					}
					for pos+1 < len(ts) && ts[pos+1] != ")" {
						pos++
						if pos+2 >= len(ts) || ts[pos+1] != "=" {
							return none, fmt.Errorf("invalid properties")
						}
						props[strings.Trim(ts[pos], "'\"")] = strings.Trim(ts[pos+2], "'\"")
						pos += 2
						if pos+1 < len(ts) && ts[pos+1] == "," {
							pos++
						}
					}
					pos += 2
				default:
					return none, fmt.Errorf("UnsupportedFeature: CREATE suffix %s", ts[pos])
				}
			}
		}
		for _, k := range primaryKeys {
			found := false
			for _, c := range cols {
				if c.Name == k {
					found = true
				}
			}
			if !found {
				return none, fmt.Errorf("unknown primary key %s", k)
			}
		}
		if _, err = load(ctx, tx, ns, name); err == nil {
			if exists {
				return none, nil
			}
			return none, fmt.Errorf("TableAlreadyExists: %s", name)
		}
		defs := []string{}
		seen := map[string]bool{}
		for _, c := range append(append([]Column{}, cols...), parts...) {
			if seen[c.Name] {
				return none, fmt.Errorf("duplicate column")
			}
			seen[c.Name] = true
			def := Quote(c.Name) + " " + c.Parsed.Duck()
			if !c.Nullable {
				def += " NOT NULL"
			}
			defs = append(defs, def)
		}
		if _, err = tx.ExecContext(ctx, "CREATE TABLE "+Quote(name)+" ("+strings.Join(defs, ",")+")"); err != nil {
			return none, err
		}
		tableID, err := newTableID()
		if err != nil {
			return none, err
		}
		b, _ := json.Marshal(Table{ID: tableID, Name: name, Columns: cols, Partitions: parts, PrimaryKeys: primaryKeys, Properties: props, Created: time.Now().Unix()})
		_, err = tx.ExecContext(ctx, "INSERT INTO main.emulator_catalog VALUES(?,?,?)", ns, name, string(b))
		return none, err
	case "drop":
		if word(ts[1]) == "function" {
			return none, fmt.Errorf("UnsupportedFeature: DROP FUNCTION; drop UDFs through DELETE on the /registration/functions REST API")
		}
		if len(ts) < 3 || word(ts[1]) != "table" {
			return none, fmt.Errorf("UnsupportedFeature: DROP")
		}
		pos := 2
		if word(ts[pos]) == "if" {
			pos += 2
		}
		name, end, err := nameAt(ts, pos)
		if err != nil || end != len(ts) {
			return none, fmt.Errorf("invalid DROP")
		}
		if _, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+Quote(name)); err != nil {
			return none, err
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM main.emulator_catalog WHERE namespace=? AND name=?", ns, name)
		return none, err
	case "truncate":
		if len(ts) < 3 || word(ts[1]) != "table" {
			return none, fmt.Errorf("invalid TRUNCATE")
		}
		name, end, err := nameAt(ts, 2)
		if err != nil || end != len(ts) {
			return none, fmt.Errorf("invalid TRUNCATE target")
		}
		table, err := load(ctx, tx, ns, name)
		if err != nil {
			return none, err
		}
		table.EmptyPartitions, err = partitionList(ctx, tx, table)
		if err != nil {
			return none, err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+Quote(name)); err != nil {
			return none, err
		}
		return none, saveTable(ctx, tx, ns, table)
	case "insert":
		if len(ts) < 4 {
			return none, fmt.Errorf("invalid INSERT")
		}
		overwrite := word(ts[1]) == "overwrite"
		if !overwrite && word(ts[1]) != "into" {
			return none, fmt.Errorf("invalid INSERT mode")
		}
		pos := 2
		if word(ts[pos]) == "table" {
			pos++
		}
		name, pos, err := nameAt(ts, pos)
		if err != nil {
			return none, err
		}
		table, err := load(ctx, tx, ns, name)
		if err != nil {
			return none, err
		}
		part := map[string]string{}
		if pos < len(ts) && word(ts[pos]) == "partition" {
			part, pos, err = parsePartition(ts, pos+1)
			if err != nil {
				return none, err
			}
		}
		if len(part) != len(table.Partitions) {
			return none, fmt.Errorf("partition spec required and must be complete")
		}
		if pos >= len(ts) {
			return none, fmt.Errorf("missing INSERT source")
		}
		source := render(ts[pos:])
		if word(ts[pos]) != "values" && word(ts[pos]) != "select" && word(ts[pos]) != "with" {
			return none, fmt.Errorf("UnsupportedFeature: INSERT source")
		}
		table.EmptyPartitions, err = partitionList(ctx, tx, table)
		if err != nil {
			return none, err
		}
		if len(part) > 0 {
			table.EmptyPartitions = append(table.EmptyPartitions, part)
		}
		// Snapshot the input first: INSERT OVERWRITE t SELECT ... FROM t must see old t.
		if _, err = tx.ExecContext(ctx, "CREATE TEMP TABLE __emulator_stage AS "+source); err != nil {
			return none, err
		}
		defer tx.ExecContext(ctx, "DROP TABLE IF EXISTS __emulator_stage")
		extra := []string{}
		args := []any{}
		where := []string{}
		for _, c := range table.Partitions {
			v, ok := part[c.Name]
			if !ok {
				return none, fmt.Errorf("missing partition %s", c.Name)
			}
			extra = append(extra, "CAST(? AS "+c.Parsed.Duck()+")")
			args = append(args, v)
			where = append(where, Quote(c.Name)+"=CAST(? AS "+c.Parsed.Duck()+")")
		}
		if overwrite {
			q := "DELETE FROM " + Quote(name)
			if len(where) > 0 {
				q += " WHERE " + strings.Join(where, " AND ")
			}
			if _, err = tx.ExecContext(ctx, q, args...); err != nil {
				return none, err
			}
		}
		q := "INSERT INTO " + Quote(name) + " SELECT *"
		if len(extra) > 0 {
			q += "," + strings.Join(extra, ",")
		}
		q += " FROM __emulator_stage"
		_, err = tx.ExecContext(ctx, q, args...)
		if err == nil {
			err = saveTable(ctx, tx, ns, table)
		}
		return none, err
	case "select", "with":
		return e.query(ctx, tx, render(ts))
	default:
		return none, fmt.Errorf("UnsupportedFeature: %s", ts[0])
	}
}
func (e *Engine) query(ctx context.Context, tx *sql.Tx, q string, args ...any) (Result, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	if err != nil {
		return Result{}, err
	}
	r := Result{Rows: [][]any{}}
	for _, c := range cts {
		typ := strings.ToLower(c.DatabaseTypeName())
		switch typ {
		case "varchar":
			typ = "string"
		case "integer":
			typ = "int"
		case "timestamp_ms":
			typ = "datetime"
		case "timestamp_ns":
			typ = "timestamp"
		}
		col, e := NewColumn(c.Name(), typ)
		if e != nil {
			col = Column{Name: c.Name(), Type: typ}
		}
		r.Columns = append(r.Columns, col)
	}
	for rows.Next() {
		if len(r.Rows) >= e.maxRows {
			return Result{}, fmt.Errorf("ResourceLimit: row limit %d", e.maxRows)
		}
		v := make([]any, len(cts))
		ptr := make([]any, len(cts))
		for i := range v {
			ptr[i] = &v[i]
		}
		if err = rows.Scan(ptr...); err != nil {
			return Result{}, err
		}
		for i := range v {
			var size int64
			v[i], size = normalize(v[i])
			r.Bytes += size
		}
		if r.Bytes > 64<<20 {
			return Result{}, fmt.Errorf("ResourceLimit: result exceeds 64 MiB")
		}
		r.Rows = append(r.Rows, v)
	}
	return r, rows.Err()
}
func (e *Engine) Snapshot(ctx context.Context, p, s, name string, part map[string]string) (Result, Table, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, ns, err := e.transaction(ctx, p, s)
	if err != nil {
		return Result{}, Table{}, err
	}
	defer tx.Rollback()
	t, err := load(ctx, tx, ns, name)
	if err != nil {
		return Result{}, t, err
	}
	columns := []string{}
	for _, c := range t.Columns {
		columns = append(columns, Quote(c.Name))
	}
	where := []string{}
	args := []any{}
	if len(t.Partitions) != len(part) {
		return Result{}, t, fmt.Errorf("InvalidPartition: complete partition spec required")
	}
	for _, c := range t.Partitions {
		v, ok := part[c.Name]
		if !ok {
			return Result{}, t, fmt.Errorf("InvalidPartition: missing key")
		}
		where = append(where, Quote(c.Name)+"=CAST(? AS "+c.Parsed.Duck()+")")
		args = append(args, v)
	}
	if len(part) > 0 {
		parts, err := partitionList(ctx, tx, t)
		if err != nil {
			return Result{}, t, err
		}
		found := false
		for _, existing := range parts {
			match := true
			for k, v := range part {
				if existing[k] != v {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		if !found {
			return Result{}, t, fmt.Errorf("NoSuchPartition: The specified partition does not exist.")
		}
	}
	q := "SELECT " + strings.Join(columns, ",") + " FROM " + Quote(t.Name)
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	r, err := e.query(ctx, tx, q, args...)
	r.Columns = t.Columns
	return r, t, err
}

// ParsePartition parses the Tunnel HTTP syntax (SDKs send unquoted values).
// SQL partition clauses are parsed separately and require SQL literals.
func ParsePartition(s string) (map[string]string, error) {
	out := map[string]string{}
	if s == "" {
		return out, nil
	}
	for _, item := range strings.Split(s, ",") {
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid partition spec")
		}
		key, value := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if key == "" {
			return nil, fmt.Errorf("empty partition key")
		}
		for _, c := range key {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
				return nil, fmt.Errorf("invalid partition key")
			}
		}
		key = strings.ToLower(key)
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate partition key")
		}
		if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
			value = strings.ReplaceAll(value[1:len(value)-1], "''", "'")
		}
		out[key] = value
	}
	return out, nil
}

// normalize keeps database-specific values out of the protocol layer and
// accounts for variable-size data retained by immutable download snapshots.
func normalize(v any) (any, int64) {
	switch x := v.(type) {
	case duckdb.OrderedMap:
		out := map[any]any{}
		keys, vals := x.Keys(), x.Values()
		size := int64(48)
		for i, k := range keys {
			nk, a := normalize(k)
			nv, b := normalize(vals[i])
			out[nk] = nv
			size += a + b + 32
		}
		return out, size
	case []any:
		out := make([]any, len(x))
		size := int64(24)
		for i, item := range x {
			n, b := normalize(item)
			out[i] = n
			size += b + 16
		}
		return out, size
	case map[string]any:
		out := map[string]any{}
		size := int64(48)
		for k, item := range x {
			n, b := normalize(item)
			out[k] = n
			size += int64(len(k)) + b + 32
		}
		return out, size
	case string:
		return v, int64(len(x)) + 16
	case []byte:
		return v, int64(len(x)) + 24
	default:
		return v, 32
	}
}

// HasProject includes projects provisioned by successful fixture execution.
func (e *Engine) HasProject(ctx context.Context, project string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var found bool
	err := e.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM main.emulator_projects WHERE name=?)", project).Scan(&found)
	return found, err
}
