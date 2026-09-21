package engine

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Resource and Function metadata implement the object half of the UDF
// workflow: a function references resources by name, and both live in the
// same project/schema namespace as tables. Request and response shapes follow
// the public Java SDK (Resources, Functions, ResourceBuilder) and PyODPS
// (odps/models/resources.py, odps/models/function.py) REST contracts.
//
// Names resolve case-insensitively, matching MaxCompute identifier handling;
// the stored display name keeps the case the caller used.
// Resource payload budgets. MaxResourceContent matches the request body limit
// in internal/wire; the namespace total keeps an in-memory container from
// being filled by uploads. Both are variables so tests can exercise the
// rejection paths without allocating hundreds of megabytes.
var (
	MaxResourceContent = int64(64 << 20)
	MaxResourceTotal   = int64(512 << 20)
)

// Resource is file-like metadata; the payload is stored beside it and read
// separately so listing never materialises content.
type Resource struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Comment string `json:"comment,omitempty"`
	IsTemp  bool   `json:"is_temp,omitempty"`
	Size    int64  `json:"size"`
	// MD5 is the hex digest of the payload; the SDKs surface it as Content-MD5.
	MD5 string `json:"md5,omitempty"`
	// Created and Updated are epoch milliseconds.
	Created   int64  `json:"created"`
	Updated   int64  `json:"updated"`
	TableName string `json:"table_name,omitempty"`
}

// Function is a registered UDF alias. Java/PyODPS both carry the alias in the
// XML element named Alias, so the field keeps that wording.
type Function struct {
	Name        string   `json:"name"`
	Owner       string   `json:"owner,omitempty"`
	ClassType   string   `json:"class_type,omitempty"`
	Resources   []string `json:"resources,omitempty"`
	Created     int64    `json:"created"`
	SQLFunction bool     `json:"sql_function,omitempty"`
	SQLText     string   `json:"sql_text,omitempty"`
	Embedded    bool     `json:"embedded,omitempty"`
	Language    string   `json:"language,omitempty"`
	Code        string   `json:"code,omitempty"`
	FileName    string   `json:"file_name,omitempty"`
}

func validResourceType(t string) bool {
	switch t {
	case "FILE", "JAR", "PY", "ARCHIVE", "TABLE":
		return true
	}
	return false
}

func resourceName(s string) (string, error) {
	n := strings.TrimSpace(s)
	if n == "" || len(n) > 255 || strings.ContainsAny(n, "/\\\r\n\x00") {
		return "", fmt.Errorf("InvalidParameter: resource name must be 1..255 characters without path separators")
	}
	return n, nil
}

func (e *Engine) lookupResource(ctx context.Context, ns, key string) (Resource, []byte, error) {
	var r Resource
	var raw string
	var content []byte
	err := e.db.QueryRowContext(ctx, "SELECT definition, content FROM main.emulator_resources WHERE namespace=? AND name=?", ns, key).Scan(&raw, &content)
	if err == sql.ErrNoRows {
		return r, nil, fmt.Errorf("NoSuchResource: %s", key)
	}
	if err != nil {
		return r, nil, err
	}
	if err = json.Unmarshal([]byte(raw), &r); err != nil {
		return r, nil, err
	}
	return r, content, nil
}

// ResourceMode distinguishes the create, replace and upsert semantics the
// public SDKs drive: POST creates, PUT replaces, and chunked part uploads
// re-send the same deterministic part name on retry, so parts upsert.
type ResourceMode int

const (
	ResourceCreate ResourceMode = iota
	ResourceReplace
	ResourceUpsert
)

// PutResource stores a resource payload. Table resources keep only metadata;
// their rows stay in the table.
func (e *Engine) PutResource(ctx context.Context, p, s, name, typ, comment string, isTemp bool, tableSource string, content []byte, mode ResourceMode) (Resource, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := resourceName(name)
	if err != nil {
		return Resource{}, err
	}
	typ = strings.ToUpper(strings.TrimSpace(typ))
	if !validResourceType(typ) {
		return Resource{}, fmt.Errorf("InvalidResourceType: %s (expected FILE, JAR, PY, ARCHIVE or TABLE)", typ)
	}
	key := strings.ToLower(n)
	ns := namespace(p, s)
	if typ == "TABLE" {
		if tableSource == "" {
			return Resource{}, fmt.Errorf("InvalidParameter: TABLE resource requires a source table")
		}
		content = nil
	} else if tableSource != "" {
		return Resource{}, fmt.Errorf("InvalidParameter: only TABLE resources carry a source table")
	}
	if int64(len(content)) > MaxResourceContent {
		return Resource{}, fmt.Errorf("ResourceOverSize: %s exceeds %d bytes", n, MaxResourceContent)
	}
	var total int64
	if err = e.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(size),0) FROM main.emulator_resources WHERE namespace=?", ns).Scan(&total); err != nil {
		return Resource{}, err
	}
	now := time.Now().UnixMilli()
	sum := md5.Sum(content)
	r := Resource{Name: n, Type: typ, Comment: comment, IsTemp: isTemp, Size: int64(len(content)), Created: now, Updated: now, TableName: tableSource, MD5: hex.EncodeToString(sum[:])}
	found, _, existsErr := e.lookupResource(ctx, ns, key)
	if existsErr != nil && !strings.HasPrefix(existsErr.Error(), "NoSuchResource:") {
		return Resource{}, existsErr
	}
	missing := existsErr != nil
	if missing && mode == ResourceReplace {
		return Resource{}, fmt.Errorf("NoSuchResource: %s", n)
	}
	if !missing && mode == ResourceCreate {
		return Resource{}, fmt.Errorf("ResourceAlreadyExists: %s", n)
	}
	replaced := int64(0)
	if !missing {
		if found.Type != typ && mode != ResourceUpsert {
			return Resource{}, fmt.Errorf("InvalidResourceType: %s already exists as %s", n, found.Type)
		}
		replaced = found.Size
		r.Created = found.Created
		if r.Comment == "" {
			r.Comment, r.IsTemp = found.Comment, found.IsTemp
		}
	}
	if total-replaced+r.Size > MaxResourceTotal {
		return Resource{}, fmt.Errorf("ResourceOverSize: project resource limit %d bytes exceeded", MaxResourceTotal)
	}
	raw, _ := json.Marshal(r)
	if missing {
		_, err = e.db.ExecContext(ctx, "INSERT INTO main.emulator_resources VALUES(?,?,?,?,?)", ns, key, r.Size, string(raw), content)
	} else {
		_, err = e.db.ExecContext(ctx, "UPDATE main.emulator_resources SET size=?, definition=?, content=? WHERE namespace=? AND name=?", r.Size, string(raw), content, ns, key)
	}
	return r, err
}

// Resource returns metadata only.
func (e *Engine) Resource(ctx context.Context, p, s, name string) (Resource, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, _, err := e.lookupResource(ctx, namespace(p, s), strings.ToLower(strings.TrimSpace(name)))
	return r, err
}

// ResourceContent returns the payload; range slicing happens in the handler.
func (e *Engine) ResourceContent(ctx context.Context, p, s, name string) (Resource, []byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, c, err := e.lookupResource(ctx, namespace(p, s), strings.ToLower(strings.TrimSpace(name)))
	if err != nil {
		return r, nil, err
	}
	if r.Type == "TABLE" {
		return r, nil, fmt.Errorf("UnsupportedOperation: TABLE resource %s has no downloadable content", r.Name)
	}
	return r, c, nil
}

// Resources lists metadata ordered by name.
func (e *Engine) Resources(ctx context.Context, p, s string) ([]Resource, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rows, err := e.db.QueryContext(ctx, "SELECT definition FROM main.emulator_resources WHERE namespace=? ORDER BY name", namespace(p, s))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Resource{}
	for rows.Next() {
		var raw string
		var r Resource
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (e *Engine) DeleteResource(ctx context.Context, p, s, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	res, err := e.db.ExecContext(ctx, "DELETE FROM main.emulator_resources WHERE namespace=? AND name=?", namespace(p, s), strings.ToLower(strings.TrimSpace(name)))
	if err != nil {
		return err
	}
	return affectedNotFound(res, "NoSuchResource", name)
}

// PutFunction creates (overwrite=false) or replaces (overwrite=true) a
// function alias. Referenced resources must already exist so that a
// registered UDF is always self-consistent.
func (e *Engine) PutFunction(ctx context.Context, p, s string, f Function, overwrite bool) (Function, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := functionName(f.Name)
	if err != nil {
		return Function{}, err
	}
	f.Name = n
	f.Resources = dedupeNames(f.Resources)
	if !f.SQLFunction && !f.Embedded {
		if strings.TrimSpace(f.ClassType) == "" {
			return Function{}, fmt.Errorf("InvalidParameter: function %s requires ClassType", n)
		}
		if len(f.Resources) == 0 {
			return Function{}, fmt.Errorf("InvalidParameter: function %s requires at least one resource", n)
		}
	}
	if f.SQLFunction && strings.TrimSpace(f.SQLText) == "" {
		return Function{}, fmt.Errorf("InvalidParameter: SQL function %s requires definition text", n)
	}
	if f.Embedded && (strings.TrimSpace(f.Code) == "" || strings.TrimSpace(f.Language) == "") {
		return Function{}, fmt.Errorf("InvalidParameter: embedded function %s requires program language and code", n)
	}
	ns := namespace(p, s)
	key := strings.ToLower(n)
	for _, res := range f.Resources {
		if _, _, e := e.lookupResource(ctx, ns, strings.ToLower(strings.TrimSpace(res))); e != nil {
			return Function{}, fmt.Errorf("InvalidParameter: function %s references unavailable resource %s", n, res)
		}
	}
	f.Created = time.Now().UnixMilli()
	existing, existsErr := e.lookupFunction(ctx, ns, key)
	if existsErr == nil {
		if !overwrite {
			return Function{}, fmt.Errorf("FunctionAlreadyExists: %s", n)
		}
		f.Created = existing.Created
	} else if !strings.HasPrefix(existsErr.Error(), "NoSuchFunction:") {
		return Function{}, existsErr
	} else if overwrite {
		return Function{}, fmt.Errorf("NoSuchFunction: %s", n)
	}
	if f.Owner == "" {
		f.Owner = "emulator"
	}
	raw, _ := json.Marshal(f)
	if existsErr == nil {
		_, err = e.db.ExecContext(ctx, "UPDATE main.emulator_functions SET definition=? WHERE namespace=? AND name=?", string(raw), ns, key)
	} else {
		_, err = e.db.ExecContext(ctx, "INSERT INTO main.emulator_functions VALUES(?,?,?)", ns, key, string(raw))
	}
	return f, err
}

func (e *Engine) lookupFunction(ctx context.Context, ns, key string) (Function, error) {
	var raw string
	f := Function{}
	err := e.db.QueryRowContext(ctx, "SELECT definition FROM main.emulator_functions WHERE namespace=? AND name=?", ns, key).Scan(&raw)
	if err == sql.ErrNoRows {
		return f, fmt.Errorf("NoSuchFunction: %s", key)
	}
	if err != nil {
		return f, err
	}
	err = json.Unmarshal([]byte(raw), &f)
	return f, err
}

// Function returns one function by alias, case-insensitively.
func (e *Engine) Function(ctx context.Context, p, s, name string) (Function, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lookupFunction(ctx, namespace(p, s), strings.ToLower(strings.TrimSpace(name)))
}

// Functions lists functions ordered by alias.
func (e *Engine) Functions(ctx context.Context, p, s string) ([]Function, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rows, err := e.db.QueryContext(ctx, "SELECT definition FROM main.emulator_functions WHERE namespace=? ORDER BY name", namespace(p, s))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Function{}
	for rows.Next() {
		var raw string
		var f Function
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (e *Engine) DeleteFunction(ctx context.Context, p, s, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	res, err := e.db.ExecContext(ctx, "DELETE FROM main.emulator_functions WHERE namespace=? AND name=?", namespace(p, s), strings.ToLower(strings.TrimSpace(name)))
	if err != nil {
		return err
	}
	return affectedNotFound(res, "NoSuchFunction", name)
}

func affectedNotFound(res sql.Result, code, name string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s: %s", code, name)
	}
	return nil
}

func functionName(s string) (string, error) {
	n := strings.TrimSpace(s)
	if n == "" || len(n) > 255 || strings.ContainsAny(n, "/\\\r\n\x00") {
		return "", fmt.Errorf("InvalidParameter: function alias must be 1..255 characters without path separators")
	}
	return n, nil
}

func dedupeNames(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" || seen[strings.ToLower(v)] {
			continue
		}
		seen[strings.ToLower(v)] = true
		out = append(out, v)
	}
	return out
}
