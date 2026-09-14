package engine

import (
	"context"
	"fmt"
	"strings"
)

// SnapshotStorage includes partition columns and accepts partial partition filters.
func (e *Engine) SnapshotStorage(ctx context.Context, p, s, name string, partitions []map[string]string) (Result, Table, error) {
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
	cols := append(append([]Column{}, t.Columns...), t.Partitions...)
	names := []string{}
	for _, c := range cols {
		names = append(names, Quote(c.Name))
	}
	ors := []string{}
	args := []any{}
	for _, part := range partitions {
		ands := []string{}
		for k, v := range part {
			found := false
			for _, c := range t.Partitions {
				if c.Name == k {
					ands = append(ands, Quote(k)+"=CAST(? AS "+c.Parsed.Duck()+")")
					args = append(args, v)
					found = true
					break
				}
			}
			if !found {
				return Result{}, t, fmt.Errorf("unknown partition %s", k)
			}
		}
		if len(ands) == 0 {
			ands = append(ands, "true")
		}
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	q := "SELECT " + strings.Join(names, ",") + " FROM " + Quote(name)
	if len(ors) > 0 {
		q += " WHERE " + strings.Join(ors, " OR ")
	}
	r, err := e.query(ctx, tx, q, args...)
	r.Columns = cols
	return r, t, err
}
