package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func partitionKey(t Table, p map[string]string) (string, error) {
	if len(p) != len(t.Partitions) || len(p) == 0 {
		return "", fmt.Errorf("complete partition required")
	}
	var keys []string
	for _, c := range t.Partitions {
		v, ok := p[c.Name]
		if !ok {
			return "", fmt.Errorf("missing partition %s", c.Name)
		}
		keys = append(keys, c.Name+"='"+strings.ReplaceAll(v, "'", "''")+"'")
	}
	return strings.Join(keys, ","), nil
}
func (e *Engine) alterMetadata(ctx context.Context, tx *sql.Tx, ns string, ts []string) error {
	if len(ts) < 6 || word(ts[1]) != "table" {
		return fmt.Errorf("UnsupportedFeature: ALTER")
	}
	name, pos, err := nameAt(ts, 2)
	if err != nil {
		return err
	}
	t, err := load(ctx, tx, ns, name)
	if err != nil {
		return err
	}
	if pos >= len(ts) {
		return fmt.Errorf("missing ALTER action")
	}
	action := word(ts[pos])
	pos++
	if action != "add" && action != "drop" {
		return fmt.Errorf("UnsupportedFeature: ALTER %s", action)
	}
	tolerant := false
	if pos < len(ts) && word(ts[pos]) == "if" {
		tolerant = true
		pos++
		if pos < len(ts) && word(ts[pos]) == "not" {
			pos++
		}
		if pos >= len(ts) || word(ts[pos]) != "exists" {
			return fmt.Errorf("invalid IF clause")
		}
		pos++
	}
	if pos >= len(ts) || word(ts[pos]) != "partition" {
		return fmt.Errorf("expected PARTITION")
	}
	part, end, err := parsePartition(ts, pos+1)
	if err != nil {
		return err
	}
	if end != len(ts) {
		return fmt.Errorf("unsupported ALTER suffix")
	}
	key, err := partitionKey(t, part)
	if err != nil {
		return err
	}
	all, err := partitionList(ctx, tx, t)
	if err != nil {
		return err
	}
	found := false
	for _, p := range all {
		k, _ := partitionKey(t, p)
		if k == key {
			found = true
		}
	}
	if action == "add" {
		if found && !tolerant {
			return fmt.Errorf("PartitionAlreadyExists: %s", key)
		}
		if !found {
			t.EmptyPartitions = append(t.EmptyPartitions, part)
		}
	} else {
		if !found && !tolerant {
			return fmt.Errorf("NoSuchPartition: %s", key)
		}
		var keep []map[string]string
		for _, p := range t.EmptyPartitions {
			k, _ := partitionKey(t, p)
			if k != key {
				keep = append(keep, p)
			}
		}
		t.EmptyPartitions = keep
		var where []string
		var args []any
		for _, c := range t.Partitions {
			where = append(where, Quote(c.Name)+"=CAST(? AS "+c.Parsed.Duck()+")")
			args = append(args, part[c.Name])
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+Quote(t.Name)+" WHERE "+strings.Join(where, " AND "), args...); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE main.emulator_catalog SET definition=? WHERE namespace=? AND name=?", string(raw), ns, name)
	return err
}
func partitionList(ctx context.Context, tx *sql.Tx, t Table) ([]map[string]string, error) {
	out := []map[string]string{}
	seen := map[string]bool{}
	add := func(p map[string]string) {
		k, _ := partitionKey(t, p)
		if !seen[k] {
			seen[k] = true
			out = append(out, p)
		}
	}
	for _, p := range t.EmptyPartitions {
		add(p)
	}
	if len(t.Partitions) == 0 {
		return out, nil
	}
	var cols []string
	for _, c := range t.Partitions {
		cols = append(cols, "CAST("+Quote(c.Name)+" AS VARCHAR)")
	}
	rows, err := tx.QueryContext(ctx, "SELECT DISTINCT "+strings.Join(cols, ",")+" FROM "+Quote(t.Name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		ptr := make([]any, len(cols))
		for i := range ptr {
			ptr[i] = &values[i]
		}
		if err = rows.Scan(ptr...); err != nil {
			return nil, err
		}
		p := map[string]string{}
		for i, c := range t.Partitions {
			p[c.Name] = values[i].String
		}
		add(p)
	}
	sort.Slice(out, func(i, j int) bool { a, _ := partitionKey(t, out[i]); b, _ := partitionKey(t, out[j]); return a < b })
	return out, rows.Err()
}
func (e *Engine) Partitions(ctx context.Context, p, s, name string) ([]map[string]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, ns, err := e.transaction(ctx, p, s)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	t, err := load(ctx, tx, ns, name)
	if err != nil {
		return nil, err
	}
	return partitionList(ctx, tx, t)
}

// SDK DDL uses project.schema.table; only the selected namespace is accepted.
func localQualifiedNames(ts []string, p, s string) []string {
	if len(ts) < 3 {
		return ts
	}
	pos := 2
	switch word(ts[0]) {
	case "create":
		if word(ts[1]) != "table" {
			return ts
		}
		if word(ts[pos]) == "if" {
			pos += 3
		}
	case "drop":
		if word(ts[1]) != "table" {
			return ts
		}
		if word(ts[pos]) == "if" {
			pos += 2
		}
	case "alter", "truncate":
		if word(ts[1]) != "table" {
			return ts
		}
	case "insert":
		if word(ts[pos]) == "table" {
			pos++
		}
	default:
		return ts
	}
	skip := 0
	if pos+4 < len(ts) && word(ts[pos]) == word(p) && ts[pos+1] == "." && word(ts[pos+2]) == word(s) && ts[pos+3] == "." {
		skip = 4
	} else if s == "default" && pos+2 < len(ts) && word(ts[pos]) == word(p) && ts[pos+1] == "." && (pos+3 >= len(ts) || ts[pos+3] != ".") {
		skip = 2
	}
	if skip == 0 {
		return ts
	}
	out := append([]string{}, ts[:pos]...)
	return append(out, ts[pos+skip:]...)
}

func saveTable(ctx context.Context, tx *sql.Tx, ns string, t Table) error {
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE main.emulator_catalog SET definition=? WHERE namespace=? AND name=?", string(raw), ns, t.Name)
	return err
}
