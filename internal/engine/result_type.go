package engine

import (
	"fmt"
	"strings"
)

// DuckDB exposes nested query column types in its own SQL syntax. Tunnel metadata
// must use ODPS type names recursively, including leaves inside arrays and structs.
func resultType(duck string) (string, error) {
	s := strings.TrimSpace(duck)
	if strings.HasSuffix(s, "[]") {
		child, err := resultType(strings.TrimSuffix(s, "[]"))
		return "array<" + child + ">", err
	}
	if i := strings.IndexByte(s, '('); i >= 0 && strings.HasSuffix(s, ")") {
		kind := strings.ToLower(s[:i])
		body := s[i+1 : len(s)-1]
		switch kind {
		case "map":
			args := splitTypes(body)
			if len(args) != 2 {
				return "", fmt.Errorf("invalid MAP result type")
			}
			key, err := resultType(args[0])
			if err != nil {
				return "", err
			}
			value, err := resultType(args[1])
			return "map<" + key + "," + value + ">", err
		case "struct":
			var fields []string
			for _, field := range splitTypes(body) {
				field = strings.TrimSpace(field)
				end := strings.IndexByte(field, ' ')
				if strings.HasPrefix(field, `"`) {
					end = 1
					for end < len(field) {
						if field[end] == '"' {
							if end+1 < len(field) && field[end+1] == '"' {
								end += 2
								continue
							}
							end++
							break
						}
						end++
					}
				}
				if end < 1 || end >= len(field) {
					return "", fmt.Errorf("invalid STRUCT result type")
				}
				name := strings.ReplaceAll(strings.Trim(field[:end], `"`), `""`, `"`)
				child, err := resultType(field[end:])
				if err != nil {
					return "", err
				}
				fields = append(fields, "`"+name+"`:"+child)
			}
			return "struct<" + strings.Join(fields, ",") + ">", nil
		}
	}
	s = strings.ToLower(s)
	switch s {
	case "varchar":
		s = "string"
	case "integer":
		s = "int"
	case "blob":
		s = "binary"
	case "timestamp_ms":
		s = "datetime"
	case "timestamp_ns":
		s = "timestamp"
	}
	_, err := ParseType(s)
	return s, err
}
