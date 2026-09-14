package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

type Type struct {
	Name             string
	Precision, Scale int
	Children         []Type
	Fields           []string
}
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Comment  string `json:"comment"`
	Parsed   Type   `json:"-"`
}
type Table struct {
	ID                  string
	Properties          map[string]string
	Created             int64
	EmptyPartitions     []map[string]string
	PrimaryKeys         []string
	Name                string
	Columns, Partitions []Column
}
type Result struct {
	Bytes   int64
	Columns []Column
	Rows    [][]any
}

func ParseType(s string) (Type, error) {
	s = strings.ToLower(strings.ReplaceAll(s, " ", ""))
	t := Type{Name: s}
	if i := strings.IndexAny(s, "(<"); i >= 0 {
		t.Name = s[:i]
		body := s[i+1 : len(s)-1]
		parts := splitTypes(body)
		switch t.Name {
		case "decimal":
			if len(parts) != 2 {
				return t, fmt.Errorf("DECIMAL requires precision,scale")
			}
			var e error
			t.Precision, e = strconv.Atoi(parts[0])
			if e != nil {
				return t, e
			}
			t.Scale, e = strconv.Atoi(parts[1])
			if e != nil || t.Precision < 1 || t.Precision > 38 || t.Scale < 0 || t.Scale > t.Precision {
				return t, fmt.Errorf("invalid DECIMAL")
			}
		case "array", "map", "struct":
			for _, p := range parts {
				if t.Name == "struct" {
					a, b, ok := strings.Cut(p, ":")
					if !ok {
						return t, fmt.Errorf("invalid STRUCT")
					}
					t.Fields = append(t.Fields, a)
					p = b
				}
				c, e := ParseType(p)
				if e != nil {
					return t, e
				}
				t.Children = append(t.Children, c)
			}
			if (t.Name == "array" && len(t.Children) != 1) || (t.Name == "map" && len(t.Children) != 2) {
				return t, fmt.Errorf("invalid complex type")
			}
		default:
			return t, fmt.Errorf("unsupported type %s", s)
		}
	} else {
		switch s {
		case "bigint", "int", "smallint", "tinyint", "float", "double", "boolean", "string", "binary", "date", "datetime", "timestamp", "timestamp_ntz":
		default:
			return t, fmt.Errorf("unsupported type %s", s)
		}
	}
	return t, nil
}
func splitTypes(s string) []string {
	var out []string
	start, depth := 0, 0
	for i, c := range s {
		switch c {
		case '<', '(':
			depth++
		case '>', ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}
func (t Type) Duck() string {
	switch t.Name {
	case "string":
		return "VARCHAR"
	case "binary":
		return "BLOB"
	case "datetime":
		return "TIMESTAMP_MS"
	case "timestamp", "timestamp_ntz":
		return "TIMESTAMP_NS"
	case "decimal":
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision, t.Scale)
	case "array":
		return t.Children[0].Duck() + "[]"
	case "map":
		return "MAP(" + t.Children[0].Duck() + "," + t.Children[1].Duck() + ")"
	case "struct":
		var a []string
		for i, c := range t.Children {
			a = append(a, Quote(t.Fields[i])+" "+c.Duck())
		}
		return "STRUCT(" + strings.Join(a, ",") + ")"
	}
	return strings.ToUpper(t.Name)
}
func (t Type) Arrow() arrow.DataType {
	switch t.Name {
	case "bigint":
		return arrow.PrimitiveTypes.Int64
	case "int":
		return arrow.PrimitiveTypes.Int32
	case "smallint":
		return arrow.PrimitiveTypes.Int16
	case "tinyint":
		return arrow.PrimitiveTypes.Int8
	case "float":
		return arrow.PrimitiveTypes.Float32
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "string", "binary":
		return arrow.BinaryTypes.Binary
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "datetime":
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	case "timestamp", "timestamp_ntz":
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "decimal":
		return &arrow.Decimal128Type{Precision: int32(t.Precision), Scale: int32(t.Scale)}
	case "array":
		return arrow.ListOf(t.Children[0].Arrow())
	case "map":
		return arrow.MapOf(t.Children[0].Arrow(), t.Children[1].Arrow())
	case "struct":
		var f []arrow.Field
		for i, c := range t.Children {
			f = append(f, arrow.Field{Name: t.Fields[i], Type: c.Arrow(), Nullable: true})
		}
		return arrow.StructOf(f...)
	}
	panic("invalid canonical type")
}

// StorageArrow uses UTF-8 strings; Tunnel uses binary buffers for STRING.
func (t Type) StorageArrow() arrow.DataType {
	switch t.Name {
	case "string":
		return arrow.BinaryTypes.String
	case "array":
		return arrow.ListOf(t.Children[0].StorageArrow())
	case "map":
		return arrow.MapOf(t.Children[0].StorageArrow(), t.Children[1].StorageArrow())
	case "struct":
		fields := []arrow.Field{}
		for i, c := range t.Children {
			fields = append(fields, arrow.Field{Name: t.Fields[i], Type: c.StorageArrow(), Nullable: true})
		}
		return arrow.StructOf(fields...)
	default:
		return t.Arrow()
	}
}
func Quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func NewColumn(name, typ string) (Column, error) {
	t, e := ParseType(typ)
	return Column{Name: strings.ToLower(name), Type: strings.ToLower(typ), Nullable: true, Parsed: t}, e
}
