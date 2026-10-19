package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	ErrSchemaMissing = errors.New("schema does not exist")
	ErrSchemaExists  = errors.New("schema already exists")
	schemaName       = regexp.MustCompile(`^[a-z][a-z0-9_]{2,127}$`)
)

type Schema struct {
	Name    string
	Comment string
	Created int64
}

func (e *Engine) Schemas(ctx context.Context, project string) ([]Schema, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rows, err := e.db.QueryContext(ctx, "SELECT name, comment, created FROM main.emulator_schemas WHERE project=? UNION ALL SELECT 'default', '', 0 ORDER BY name", project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Schema
	for rows.Next() {
		var s Schema
		if err := rows.Scan(&s.Name, &s.Comment, &s.Created); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (e *Engine) CreateSchema(ctx context.Context, project, name, comment string, ifNotExists bool) error {
	name = strings.ToLower(name)
	if !schemaName.MatchString(name) {
		return fmt.Errorf("invalid schema name")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var count int
	if err := e.db.QueryRowContext(ctx, "SELECT count(*) FROM main.emulator_schemas WHERE project=? AND name=?", project, name).Scan(&count); err != nil {
		return err
	}
	if name == "default" || count > 0 {
		if ifNotExists {
			return nil
		}
		return ErrSchemaExists
	}
	_, err := e.db.ExecContext(ctx, "INSERT INTO main.emulator_schemas VALUES (?,?,?,?)", project, name, comment, time.Now().Unix())
	return err
}

func (e *Engine) DeleteSchema(ctx context.Context, project, name string) error {
	name = strings.ToLower(name)
	if name == "default" {
		return fmt.Errorf("default schema cannot be deleted")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var value string
	err = tx.QueryRowContext(ctx, "SELECT name FROM main.emulator_schemas WHERE project=? AND name=?", project, name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, table := range []string{"emulator_catalog", "emulator_resources", "emulator_functions"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM main."+table+" WHERE namespace=?", namespace(project, name)); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+Quote(namespace(project, name))+" CASCADE"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM main.emulator_schemas WHERE project=? AND name=?", project, name); err != nil {
		return err
	}
	return tx.Commit()
}
