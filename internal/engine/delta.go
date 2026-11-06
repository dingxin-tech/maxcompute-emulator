package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Delta (ACID) tables carry row-level versions so an incremental read can return
// the events published inside a version window. The version objects live beside the
// user table and are named so the SQL guard in exec() rejects any user reference:
// hidden columns use the __em_ prefix, and the sequence/shadow names start with n_.
const (
	DeltaLSNColumn = "__em_lsn"
	DeltaOpColumn  = "__em_op"
	DeltaTSColumn  = "__em_ts"

	// MetaOpColumn and MetaTSColumn are the system columns the service adds to every
	// incremental read session; the op values are the ones CDC consumers branch on.
	MetaOpColumn = "__meta_op_type"
	MetaTSColumn = "__meta_timestamp"

	deltaOpInsert = 2
	deltaOpDelete = 4
	deltaOpAfter  = 8
	deltaOpCdc    = 32
	deltaOpBefore = 16

	deltaLSNALIAS = "_dlsn"
	deltaOpALIAS  = "_dop"
	deltaTSALIAS  = "_dts"
)

// IsHiddenColumn reports the columns the storage layer adds to delta tables. They
// stay out of every user-visible projection and schema.
func IsHiddenColumn(name string) bool { return strings.HasPrefix(name, "__em_") }

// IsDelta mirrors the service gate for incremental reads: a transactional (or
// format-version 2) table with a declared primary key.
func (t Table) IsDelta() bool {
	if len(t.PrimaryKeys) == 0 {
		return false
	}
	return t.Properties["transactional"] == "true" || t.Properties["table.format.version"] == "2"
}

func (t Table) DeltaSequence() string { return "n_em_seq_" + t.ID }
func (t Table) DeltaLog() string      { return "n_emlog_" + t.ID }

// storedColumns is the physical column order of both the table and its shadow.
func storedColumns(t Table) []Column {
	return append(append([]Column{}, t.Columns...), t.Partitions...)
}

func quoteNames(cols []Column) []string {
	out := []string{}
	for _, c := range cols {
		out = append(out, Quote(c.Name))
	}
	return out
}

// deltaDefinitions returns the hidden column definitions plus the DDL needed to
// create the version objects; plain holds the user-visible column definitions.
func (t Table) deltaDefinitions() []string {
	seq := t.DeltaSequence()
	return []string{
		Quote(DeltaLSNColumn) + " BIGINT DEFAULT nextval('" + seq + "')",
		Quote(DeltaOpColumn) + " TINYINT DEFAULT " + strconv.Itoa(deltaOpInsert),
		Quote(DeltaTSColumn) + " TIMESTAMP DEFAULT current_timestamp",
	}
}

func createDeltaObjects(ctx context.Context, tx *sql.Tx, t Table, plain []string) error {
	if _, err := tx.ExecContext(ctx, "CREATE SEQUENCE "+t.DeltaSequence()); err != nil {
		return err
	}
	return createDeltaLogTable(ctx, tx, t, plain)
}

// createDeltaLogTable builds the append-only event history beside a delta table.
func createDeltaLogTable(ctx context.Context, tx *sql.Tx, t Table, plain []string) error {
	if _, err := tx.ExecContext(ctx, "CREATE SEQUENCE IF NOT EXISTS "+t.DeltaSequence()); err != nil {
		return err
	}
	defs := append(append([]string{}, plain...),
		Quote(DeltaLSNColumn)+" BIGINT",
		Quote(DeltaOpColumn)+" TINYINT",
		Quote(DeltaTSColumn)+" TIMESTAMP")
	_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+Quote(t.DeltaLog())+" ("+strings.Join(defs, ",")+")")
	return err
}

func dropDeltaObjects(ctx context.Context, tx *sql.Tx, t Table) error {
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+Quote(t.DeltaLog())); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "DROP SEQUENCE IF EXISTS "+t.DeltaSequence())
	return err
}

// logDeltaRemovedRows copies the rows a statement is about to remove into the event
// log, one fresh version per row, so the events survive the deletion.
func logDeltaRemovedRows(ctx context.Context, tx *sql.Tx, t Table, where string, args []any, op int) error {
	cols := quoteNames(storedColumns(t))
	q := "INSERT INTO " + Quote(t.DeltaLog()) + " SELECT " + strings.Join(cols, ",") +
		", nextval('" + t.DeltaSequence() + "'), " + strconv.Itoa(op) + ", current_timestamp FROM " + Quote(t.Name)
	if where != "" {
		q += " WHERE " + where
	}
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// insertDeltaRow appends one known image (a before-image) to the event log.
func insertDeltaRow(ctx context.Context, tx *sql.Tx, t Table, values []any, op int) error {
	cols := storedColumns(t)
	if len(values) != len(cols) {
		return fmt.Errorf("column count mismatch")
	}
	exprs := []string{}
	args := []any{}
	for i, c := range cols {
		e, bound := bindExpression(c.Parsed, values[i])
		exprs = append(exprs, e)
		args = append(args, bound...)
	}
	exprs = append(exprs, "nextval('"+t.DeltaSequence()+"')", strconv.Itoa(op), "current_timestamp")
	q := "INSERT INTO " + Quote(t.DeltaLog()) + " VALUES (" + strings.Join(exprs, ",") + ")"
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// logDeltaRows copies live rows into the event log. The write path calls it per row,
// right after the row is published, so no version is missed inside one batch.
func logDeltaRows(ctx context.Context, tx *sql.Tx, t Table, where string, args []any) error {
	names := quoteNames(storedColumns(t))
	q := "INSERT INTO " + Quote(t.DeltaLog()) + " SELECT " + strings.Join(names, ",") +
		", " + Quote(DeltaLSNColumn) + ", " + Quote(DeltaOpColumn) + ", " + Quote(DeltaTSColumn) +
		" FROM " + Quote(t.Name) + " WHERE " + where
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// logDeltaNewRows publishes insert events for rows no statement has logged yet. It is
// safe for one whole INSERT because nothing else writes between the insert and here.
func logDeltaNewRows(ctx context.Context, tx *sql.Tx, t Table) error {
	names := quoteNames(storedColumns(t))
	q := "INSERT INTO " + Quote(t.DeltaLog()) + " SELECT " + strings.Join(names, ",") +
		", " + Quote(DeltaLSNColumn) + ", " + Quote(DeltaOpColumn) + ", " + Quote(DeltaTSColumn) +
		" FROM " + Quote(t.Name) + " WHERE " + Quote(DeltaLSNColumn) + " > (SELECT coalesce(max(" +
		Quote(DeltaLSNColumn) + "), 0) FROM " + Quote(t.DeltaLog()) + ")"
	_, err := tx.ExecContext(ctx, q)
	return err
}

// readStoredRow fetches the physical row a predicate selects, or nil when absent.
func readStoredRow(ctx context.Context, tx *sql.Tx, t Table, where string, args []any) ([]any, error) {
	cols := storedColumns(t)
	q := "SELECT " + strings.Join(quoteNames(cols), ",") + " FROM " + Quote(t.Name) + " WHERE " + where + " LIMIT 1"
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	v := make([]any, len(cols))
	ptr := make([]any, len(cols))
	for i := range v {
		ptr[i] = &v[i]
	}
	if err = rows.Scan(ptr...); err != nil {
		return nil, err
	}
	for i := range v {
		v[i], _ = normalize(v[i])
	}
	return v, rows.Err()
}

// DeltaBound is the newest version the table has reached across the event log and the
// live rows. Rollbacks leave holes in the sequence and deletes remove the newest live
// row, so neither the sequence value nor the data table alone is a monotone bound.
func deltaBound(ctx context.Context, tx *sql.Tx, t Table) (int64, error) {
	q := "SELECT max(lsn) FROM (SELECT " + Quote(DeltaLSNColumn) + " lsn FROM " + Quote(t.Name) +
		" UNION ALL SELECT " + Quote(DeltaLSNColumn) + " lsn FROM " + Quote(t.DeltaLog()) + ")"
	var bound sql.NullInt64
	if err := tx.QueryRowContext(ctx, q).Scan(&bound); err != nil {
		return 0, err
	}
	return bound.Int64, nil
}

func partitionWhere(t Table, partitions []map[string]string) (string, []any, error) {
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
				return "", nil, fmt.Errorf("unknown partition %s", k)
			}
		}
		if len(ands) == 0 {
			ands = append(ands, "true")
		}
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	if len(ors) == 0 {
		return "", args, nil
	}
	return strings.Join(ors, " OR "), args, nil
}

// deltaColumns is the schema an incremental session publishes: data columns, then
// the system columns the service adds, then partition columns. Consumers concatenate
// the same three buckets, so the Arrow field order has to match it.
func deltaColumns(t Table) []Column {
	cols := append([]Column{}, t.Columns...)
	op, _ := NewColumn(MetaOpColumn, "tinyint")
	op.Nullable = false
	ts, _ := NewColumn(MetaTSColumn, "timestamp")
	ts.Nullable = false
	cols = append(cols, op, ts)
	return append(cols, t.Partitions...)
}

// DeltaRead returns the events a delta table published in (start, end]. A negative
// end means "up to whatever the table has reached now"; the returned version is the
// end of the window the caller consumed, which is what readers advance to. events
// adds the delete/update-before images, so append mode sees inserted rows only.
func (e *Engine) DeltaRead(ctx context.Context, p, s, name string, partitions []map[string]string, start, end int64, events bool) (Result, int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, ns, err := e.transaction(ctx, p, s)
	if err != nil {
		return Result{}, 0, err
	}
	defer tx.Rollback()
	t, err := load(ctx, tx, ns, name)
	if err != nil {
		return Result{}, 0, err
	}
	if !t.IsDelta() {
		return Result{}, 0, fmt.Errorf("Only ACID table support incremental read")
	}
	bound, err := deltaBound(ctx, tx, t)
	if err != nil {
		return Result{}, 0, err
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = bound
	}
	if end < start {
		return Result{}, 0, fmt.Errorf("invalid incremental range (%d,%d]", start, end)
	}
	where, partArgs, err := partitionWhere(t, partitions)
	if err != nil {
		return Result{}, 0, err
	}
	names := quoteNames(storedColumns(t))
	// Two different questions, two different sources, both measured against the live
	// service on 2026-10-14: cdc answers what happened from the append-only event
	// history, while append answers what is new from live rows -- so a row that was
	// inserted and later deleted inside the window appears in neither.
	src := Quote(t.Name)
	cdcCodes := false
	if events {
		src = Quote(t.DeltaLog())
		cdcCodes = true
	}
	head := "SELECT " + strings.Join(names, ",") + ", " + Quote(DeltaLSNColumn) + " " + deltaLSNALIAS +
		", " + Quote(DeltaOpColumn) + " " + deltaOpALIAS + ", " + Quote(DeltaTSColumn) + " " + deltaTSALIAS +
		" FROM " + src + " WHERE "
	if where != "" {
		head += "(" + where + ") AND "
	}
	head += Quote(DeltaLSNColumn) + " > ? AND " + Quote(DeltaLSNColumn) + " <= ?"
	args := append(append([]any{}, partArgs...), start, end)
	outer := []string{}
	for _, c := range t.Columns {
		outer = append(outer, Quote(c.Name))
	}
	outer = append(outer, deltaOpALIAS+" AS "+Quote(MetaOpColumn), deltaTSALIAS+" AS "+Quote(MetaTSColumn))
	for _, c := range t.Partitions {
		outer = append(outer, Quote(c.Name))
	}
	q := "SELECT " + strings.Join(outer, ",") + " FROM (" + head +
		") ORDER BY " + deltaLSNALIAS + ", CASE WHEN " + deltaOpALIAS + " IN (" +
		strconv.Itoa(deltaOpDelete) + "," + strconv.Itoa(deltaOpBefore) + ") THEN 0 ELSE 1 END"
	r, err := e.query(ctx, tx, q, args...)
	if err != nil {
		return Result{}, 0, err
	}
	if cdcCodes {
		// The service reports a replaced row as 32 in the CDC view while the live row
		// keeps 8; consumers fold both into UPDATE_AFTER either way.
		op := len(t.Columns)
		for _, row := range r.Rows {
			if v, ok := row[op].(int8); ok && v == deltaOpAfter {
				row[op] = int8(deltaOpCdc)
			}
		}
	}
	r.Columns = deltaColumns(t)
	for _, row := range r.Rows {
		if len(row) != len(r.Columns) {
			return Result{}, 0, fmt.Errorf("incremental event width mismatch")
		}
	}
	return r, end, nil
}

// deltaOpOf maps a write-API operation onto the event code a CDC consumer branches on.
// An upsert that found no previous row is an insert, not an update.
func DeltaOpOf(op byte, existed bool) int {
	if op == 'D' {
		return deltaOpDelete
	}
	if existed {
		return deltaOpAfter
	}
	return deltaOpInsert
}

// DeltaBeforeOp is the event code for the image an upsert replaces.
func DeltaBeforeOp(op byte) int {
	if op == 'D' {
		return deltaOpDelete
	}
	return deltaOpBefore
}

// StoredWidth tells the write path how many values a delta row must carry.
func (t Table) StoredWidth() int { return len(storedColumns(t)) }

// DeltaColumnNames lists the physical columns, excluding hidden version columns.
func (t Table) DeltaColumnNames() []string { return quoteNames(storedColumns(t)) }

// MergePartial keeps the stored value for every column an update did not mention.
// The request carries the mentioned columns only; the rest must survive the write.
func MergePartial(row, old []any, provided []int, dataColumns int) []any {
	keep := map[int]bool{}
	for _, i := range provided {
		keep[i] = true
	}
	out := append([]any{}, row...)
	for i := 0; i < dataColumns && i < len(out) && i < len(old); i++ {
		if !keep[i] {
			out[i] = old[i]
		}
	}
	return out
}

// upgradeDeltaTables repairs persisted ACID tables that predate the version objects.
// Table.IsDelta() reads the catalog, so a transactional table stored by an older release
// is already treated as a delta table, while its hidden columns, sequence and history
// table were only ever built by the CREATE path - ordinary writes, TRUNCATE and
// incremental reads would then fail on missing columns or a missing table.
//
// Explicit initial-history contract: rows that existed before the upgrade become the
// baseline snapshot at version 1, published as insert events, so no consumer is silently
// told the table was empty; the first write after the upgrade takes version 2 and lands
// in the next window. Nothing is invented for changes that happened before the upgrade.
func upgradeDeltaTables(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT namespace,name,definition FROM main.emulator_catalog")
	if err != nil {
		return err
	}
	type pending struct {
		ns string
		t  Table
	}
	list := []pending{}
	for rows.Next() {
		var ns, name, raw string
		if err = rows.Scan(&ns, &name, &raw); err != nil {
			rows.Close()
			return err
		}
		var t Table
		if err = json.Unmarshal([]byte(raw), &t); err != nil {
			rows.Close()
			return err
		}
		if t.IsDelta() && t.ID != "" {
			list = append(list, pending{ns, t})
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, item := range list {
		if err = upgradeDeltaTable(tx, item.ns, item.t); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// upgradeDeltaTable brings one persisted delta table to the current physical layout.
func upgradeDeltaTable(tx *sql.Tx, ns string, t Table) error {
	var hidden int
	if err := tx.QueryRow("SELECT count(*) FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name=?",
		ns, t.Name, DeltaLSNColumn).Scan(&hidden); err != nil {
		return err
	}
	var logs int
	if err := tx.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
		ns, t.DeltaLog()).Scan(&logs); err != nil {
		return err
	}
	if hidden > 0 && logs > 0 {
		return nil
	}
	qual := Quote(ns) + "." + Quote(t.Name)
	if hidden == 0 {
		for _, def := range []string{Quote(DeltaLSNColumn) + " BIGINT", Quote(DeltaOpColumn) + " TINYINT", Quote(DeltaTSColumn) + " TIMESTAMP"} {
			if _, err := tx.Exec("ALTER TABLE " + qual + " ADD COLUMN " + def); err != nil {
				return err
			}
		}
		var live int
		if err := tx.QueryRow("SELECT count(*) FROM " + qual).Scan(&live); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE " + qual + " SET " + Quote(DeltaLSNColumn) + "=1, " + Quote(DeltaOpColumn) + "=" +
			strconv.Itoa(deltaOpInsert) + ", " + Quote(DeltaTSColumn) + "=current_timestamp WHERE " +
			Quote(DeltaLSNColumn) + " IS NULL"); err != nil {
			return err
		}
		// The sequence has to hand out 2 and up once a baseline occupies 1.
		start := "1"
		if live > 0 {
			start = "2"
		}
		if _, err := tx.Exec("CREATE SEQUENCE IF NOT EXISTS " + t.DeltaSequence() + " START WITH " + start); err != nil {
			return err
		}
		for _, def := range []string{
			Quote(DeltaLSNColumn) + " SET DEFAULT nextval('" + t.DeltaSequence() + "')",
			Quote(DeltaOpColumn) + " SET DEFAULT " + strconv.Itoa(deltaOpInsert),
			Quote(DeltaTSColumn) + " SET DEFAULT current_timestamp",
		} {
			if _, err := tx.Exec("ALTER TABLE " + qual + " ALTER COLUMN " + def); err != nil {
				return err
			}
		}
	} else if _, err := tx.Exec("CREATE SEQUENCE IF NOT EXISTS " + t.DeltaSequence()); err != nil {
		return err
	}
	// The catalog definition is a user-visible shape and does not carry the physical
	// DuckDB types, so the history table is derived from the table as stored.
	names, decls, err := physicalColumns(tx, ns, t.Name)
	if err != nil {
		return err
	}
	plain := decls
	if logs == 0 {
		if _, err := tx.Exec("CREATE SCHEMA IF NOT EXISTS " + Quote(ns)); err != nil {
			return err
		}
		defs := append(append([]string{}, plain...),
			Quote(DeltaLSNColumn)+" BIGINT", Quote(DeltaOpColumn)+" TINYINT", Quote(DeltaTSColumn)+" TIMESTAMP")
		if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS " + Quote(ns) + "." + Quote(t.DeltaLog()) +
			" (" + strings.Join(defs, ",") + ")"); err != nil {
			return err
		}
		// Publish the baseline exactly once, carrying each row's own version.
		cols := []string{}
		for _, n := range names {
			cols = append(cols, Quote(n))
		}
		if _, err := tx.Exec("INSERT INTO " + Quote(ns) + "." + Quote(t.DeltaLog()) + " SELECT " +
			strings.Join(cols, ",") + ", " + Quote(DeltaLSNColumn) + ", " + Quote(DeltaOpColumn) + ", " +
			Quote(DeltaTSColumn) + " FROM " + qual + " WHERE " + Quote(DeltaLSNColumn) +
			" NOT IN (SELECT " + Quote(DeltaLSNColumn) + " FROM " + Quote(ns) + "." + Quote(t.DeltaLog()) + ")"); err != nil {
			return err
		}
	}
	return nil
}

// physicalColumns lists a stored table's user-visible columns with their physical
// DuckDB declarations, skipping the hidden version columns.
func physicalColumns(tx *sql.Tx, ns, name string) ([]string, []string, error) {
	rows, err := tx.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ordinal_position", ns, name)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	names, decls := []string{}, []string{}
	for rows.Next() {
		var col, typ string
		if err = rows.Scan(&col, &typ); err != nil {
			return nil, nil, err
		}
		if IsHiddenColumn(col) {
			continue
		}
		names = append(names, col)
		decls = append(decls, Quote(col)+" "+typ)
	}
	return names, decls, rows.Err()
}
