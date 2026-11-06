package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// latestVersionSelect answers the SQL function a CDC consumer polls to learn how far it
// may read: SELECT get_latest_version("<project>[.<schema>].<table>"). The number it
// reports has to be the same one an incremental session returns as LatestVersion, or a
// reader that stores one and asks with the other stalls forever.
func latestVersionSelect(ctx context.Context, tx *sql.Tx, ns string, ts []string) (Result, bool, error) {
	if len(ts) != 5 || word(ts[0]) != "select" || word(ts[1]) != "get_latest_version" || ts[2] != "(" || ts[4] != ")" {
		return Result{}, false, nil
	}
	// Quote characters are matched by code point, so this file needs no nested quoting.
	arg := strings.TrimFunc(ts[3], func(r rune) bool { return r == 39 || r == 34 || r == 96 })
	parts := strings.Split(arg, ".")
	target, name := ns, arg
	switch len(parts) {
	case 1:
		name = parts[0]
	case 2:
		target, name = namespace(parts[0], "default"), parts[1]
	case 3:
		target, name = namespace(parts[0], parts[1]), parts[2]
	default:
		return Result{}, true, fmt.Errorf("invalid table name %s", arg)
	}
	t, err := load(ctx, tx, target, name)
	if err != nil {
		return Result{}, true, err
	}
	if !t.IsDelta() {
		return Result{}, true, fmt.Errorf("get_latest_version: %s is not an ACID table with primary keys", arg)
	}
	// Read the bound through schema-qualified names: the argument may point at another
	// schema than the one this statement runs in.
	var bound sql.NullInt64
	q := "SELECT max(lsn) FROM (SELECT " + Quote(DeltaLSNColumn) + " lsn FROM " + Quote(target) + "." + Quote(t.Name) +
		" UNION ALL SELECT " + Quote(DeltaLSNColumn) + " lsn FROM " + Quote(target) + "." + Quote(t.DeltaLog()) + ")"
	if err = tx.QueryRowContext(ctx, q).Scan(&bound); err != nil {
		return Result{}, true, err
	}
	col, _ := NewColumn("_c0", "bigint")
	return Result{Columns: []Column{col}, Rows: [][]any{{bound.Int64}}, Bytes: 8}, true, nil
}
