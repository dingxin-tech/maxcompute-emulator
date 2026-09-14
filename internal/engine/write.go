package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Write atomically publishes already validated rows into a table/partition.
// An operation byte of D deletes by the declared primary key; U replaces rows.
func (e *Engine) Write(ctx context.Context, p, s, name string, part map[string]string, rows [][]any, overwrite bool, ops []byte) error {
	return e.WriteMutations(ctx, p, s, name, part, rows, overwrite, ops, nil)
}

func (e *Engine) WriteMutations(ctx context.Context, p, s, name string, part map[string]string, rows [][]any, overwrite bool, ops []byte, partial [][]int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, ns, err := e.transaction(ctx, p, s)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t, err := load(ctx, tx, ns, name)
	if err != nil {
		return err
	}
	dynamic := []Column{}
	known := map[string]bool{}
	for _, c := range t.Partitions {
		known[c.Name] = true
		if _, ok := part[c.Name]; !ok {
			dynamic = append(dynamic, c)
		}
	}
	for k := range part {
		if !known[k] {
			return fmt.Errorf("unknown partition %s", k)
		}
	}
	if len(dynamic) > 0 && (overwrite || ops != nil) {
		return fmt.Errorf("overwrite/upsert requires static partitions")
	}
	where := []string{}
	partArgs := []any{}
	for _, c := range t.Partitions {
		v, ok := part[c.Name]
		if !ok {
			continue
		}
		where = append(where, Quote(c.Name)+"="+writePlaceholder(c))
		partArgs = append(partArgs, v)
	}
	filter := ""
	if len(where) > 0 {
		filter = " WHERE " + strings.Join(where, " AND ")
	}
	if overwrite {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+Quote(name)+filter, partArgs...); err != nil {
			return err
		}
	}
	keys := []int{}
	if ops != nil {
		if len(ops) != len(rows) || len(t.PrimaryKeys) == 0 {
			return fmt.Errorf("upsert requires primary keys and operations")
		}
		for _, k := range t.PrimaryKeys {
			for i, c := range t.Columns {
				if c.Name == k {
					keys = append(keys, i)
				}
			}
		}
		if len(keys) != len(t.PrimaryKeys) {
			return fmt.Errorf("invalid primary keys")
		}
	}
	columns := append(append([]Column{}, t.Columns...), t.Partitions...)
	for i, row := range rows {
		if len(row) != len(t.Columns)+len(dynamic) {
			return fmt.Errorf("column count mismatch")
		}
		if ops != nil {
			preds := append([]string{}, where...)
			args := append([]any{}, partArgs...)
			for _, k := range keys {
				if row[k] == nil {
					return fmt.Errorf("NULL primary key")
				}
				preds = append(preds, Quote(t.Columns[k].Name)+"=CAST(? AS "+t.Columns[k].Parsed.Duck()+")")
				args = append(args, bindValue(row[k]))
			}
			if ops[i] != 'U' && ops[i] != 'D' {
				return fmt.Errorf("invalid upsert operation")
			}
			if partial != nil && i < len(partial) && len(partial[i]) > 0 && ops[i] == 'U' {
				assignments := []string{}
				values := []any{}
				seen := map[int]bool{}
				for _, idx := range partial[i] {
					if idx < 0 || idx >= len(t.Columns) || seen[idx] {
						return fmt.Errorf("invalid partial update column")
					}
					seen[idx] = true
					c := t.Columns[idx]
					expr, bound := bindExpression(c.Parsed, row[idx])
					assignments = append(assignments, Quote(c.Name)+"="+expr)
					values = append(values, bound...)
				}
				result, er := tx.ExecContext(ctx, "UPDATE "+Quote(name)+" SET "+strings.Join(assignments, ",")+" WHERE "+strings.Join(preds, " AND "), append(values, args...)...)
				if er != nil {
					return er
				}
				updated, er := result.RowsAffected()
				if er != nil {
					return er
				}
				if updated > 0 {
					continue
				}
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM "+Quote(name)+" WHERE "+strings.Join(preds, " AND "), args...); err != nil {
				return err
			}
			if ops[i] == 'D' {
				continue
			}
		}
		values := append([]any{}, row[:len(t.Columns)]...)
		di := len(t.Columns)
		for _, c := range t.Partitions {
			if v, ok := part[c.Name]; ok {
				values = append(values, v)
			} else {
				if row[di] == nil {
					return fmt.Errorf("NULL dynamic partition")
				}
				values = append(values, row[di])
				di++
			}
		}
		placeholders := []string{}
		args := []any{}
		for j, c := range columns {
			expr, bound := bindExpression(c.Parsed, values[j])
			placeholders = append(placeholders, expr)
			args = append(args, bound...)
		}
		insert := "INSERT INTO " + Quote(name) + " VALUES (" + strings.Join(placeholders, ",") + ")"
		if _, err = tx.ExecContext(ctx, insert, args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func bindValue(v any) any {
	if tm, ok := v.(time.Time); ok {
		return tm.UTC().Format("2006-01-02 15:04:05.999999999")
	}
	return v
}
func writePlaceholder(c Column) string { return "CAST(? AS " + c.Parsed.Duck() + ")" }

// Bind complex values recursively: the DuckDB driver's direct list binding
// panics on NULL elements. Every leaf remains a bound parameter, including binary.
func bindExpression(t Type, v any) (string, []any) {
	cast := func(expr string) string { return "CAST(" + expr + " AS " + t.Duck() + ")" }
	if v == nil {
		return cast("?"), []any{nil}
	}
	args := []any{}
	exprs := []string{}
	switch t.Name {
	case "array":
		a := reflect.ValueOf(v)
		for i := 0; i < a.Len(); i++ {
			expr, values := bindExpression(t.Children[0], a.Index(i).Interface())
			exprs = append(exprs, expr)
			args = append(args, values...)
		}
		return cast("[" + strings.Join(exprs, ",") + "]"), args
	case "map":
		m := reflect.ValueOf(v)
		keys := m.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i].Interface()) < fmt.Sprint(keys[j].Interface()) })
		valexpr := []string{}
		valargs := []any{}
		for _, k := range keys {
			expr, bound := bindExpression(t.Children[0], k.Interface())
			exprs = append(exprs, expr)
			args = append(args, bound...)
			expr, bound = bindExpression(t.Children[1], m.MapIndex(k).Interface())
			valexpr = append(valexpr, expr)
			valargs = append(valargs, bound...)
		}
		return cast("map([" + strings.Join(exprs, ",") + "],[" + strings.Join(valexpr, ",") + "] )"), append(args, valargs...)
	case "struct":
		m := v.(map[string]any)
		for i, name := range t.Fields {
			expr, bound := bindExpression(t.Children[i], m[name])
			exprs = append(exprs, Quote(name)+":="+expr)
			args = append(args, bound...)
		}
		return cast("struct_pack(" + strings.Join(exprs, ",") + ")"), args
	default:
		return cast("?"), []any{bindValue(v)}
	}
}
