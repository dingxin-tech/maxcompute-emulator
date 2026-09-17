package engine

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	parser "github.com/dingxin-tech/maxcompute-emulator/internal/sqlparser"
)

type syntaxErrors struct {
	*antlr.DefaultErrorListener
	message string
}

func (e *syntaxErrors) SyntaxError(_ antlr.Recognizer, _ interface{}, line, col int, msg string, _ antlr.RecognitionException) {
	if len(msg) > 180 {
		msg = msg[:180]
	}
	if e.message == "" {
		e.message = fmt.Sprintf("SQL %d:%d: %s", line, col, msg)
	}
}
func Lex(sql string) ([][]string, error) {
	if len(sql) > 1<<20 {
		return nil, fmt.Errorf("SQL exceeds 1 MiB")
	}
	if !strings.HasSuffix(strings.TrimSpace(sql), ";") {
		sql += ";"
	}
	errListener := &syntaxErrors{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	l := parser.NewOdpsLexer(antlr.NewInputStream(sql))
	l.RemoveErrorListeners()
	l.AddErrorListener(errListener)
	ts := antlr.NewCommonTokenStream(l, antlr.TokenDefaultChannel)
	p := parser.NewOdpsParser(ts)
	p.RemoveErrorListeners()
	p.AddErrorListener(errListener)
	p.Script()
	if errListener.message != "" {
		return nil, fmt.Errorf("%s", errListener.message)
	}
	ts.Fill()
	var statements [][]string
	var current []string
	for _, t := range ts.GetAllTokens() {
		if t.GetChannel() != antlr.TokenDefaultChannel || t.GetTokenType() == antlr.TokenEOF {
			continue
		}
		if t.GetText() == ";" {
			if len(current) > 0 {
				statements = append(statements, current)
				current = nil
			}
		} else {
			current = append(current, t.GetText())
		}
	}
	if len(statements) > 100 {
		return nil, fmt.Errorf("at most 100 statements")
	}
	return statements, nil
}
func word(s string) string { return strings.ToLower(strings.Trim(s, "`\"")) }
func render(ts []string) string {
	r := []string{}
	for i := 0; i < len(ts); i++ {
		s := ts[i]
		if word(s) == "map" && i+1 < len(ts) && ts[i+1] == "(" {
			end, depth := i+2, 1
			for end < len(ts) && depth > 0 {
				if ts[end] == "(" {
					depth++
				}
				if ts[end] == ")" {
					depth--
				}
				if depth > 0 {
					end++
				}
			}
			if end >= len(ts) {
				return "INVALID_MAP"
			}
			args := [][]string{}
			begin, d := i+2, 0
			for j := begin; j < end; j++ {
				if ts[j] == "(" {
					d++
				}
				if ts[j] == ")" {
					d--
				}
				if ts[j] == "," && d == 0 {
					args = append(args, ts[begin:j])
					begin = j + 1
				}
			}
			if begin < end {
				args = append(args, ts[begin:end])
			}
			if len(args)%2 != 0 {
				return "INVALID_MAP"
			}
			// Preserve the existing two-array constructor extension.
			if len(args) == 2 && len(args[0]) > 1 && len(args[1]) > 1 && word(args[0][0]) == "array" && word(args[1][0]) == "array" {
				r = append(r, "map("+render(args[0])+","+render(args[1])+")")
				i = end
				continue
			}
			keys, values := []string{}, []string{}
			for j := 0; j < len(args); j += 2 {
				keys = append(keys, render(args[j]))
				values = append(values, render(args[j+1]))
			}
			r = append(r, "map(list_value("+strings.Join(keys, ",")+"),list_value("+strings.Join(values, ",")+"))")
			i = end
			continue
		}
		if word(s) == "named_struct" && i+1 < len(ts) && ts[i+1] == "(" {
			end := i + 2
			depth := 1
			for end < len(ts) && depth > 0 {
				if ts[end] == "(" {
					depth++
				}
				if ts[end] == ")" {
					depth--
				}
				if depth > 0 {
					end++
				}
			}
			if end >= len(ts) {
				return "INVALID_NAMED_STRUCT"
			}
			args := [][]string{}
			begin, d := i+2, 0
			for j := begin; j < end; j++ {
				if ts[j] == "(" {
					d++
				}
				if ts[j] == ")" {
					d--
				}
				if ts[j] == "," && d == 0 {
					args = append(args, ts[begin:j])
					begin = j + 1
				}
			}
			args = append(args, ts[begin:end])
			fields := []string{}
			if len(args)%2 != 0 {
				return "INVALID_NAMED_STRUCT"
			}
			for j := 0; j < len(args); j += 2 {
				if len(args[j]) != 1 || !strings.HasPrefix(args[j][0], "'") {
					return "INVALID_NAMED_STRUCT"
				}
				fields = append(fields, Quote(strings.Trim(args[j][0], "'"))+" := "+render(args[j+1]))
			}
			r = append(r, "struct_pack("+strings.Join(fields, ",")+")")
			i = end
			continue
		}
		switch strings.ToLower(s) {
		case "string":
			r = append(r, "VARCHAR")
		case "datetime":
			r = append(r, "TIMESTAMP_MS")
		case "timestamp", "timestamp_ntz":
			r = append(r, "TIMESTAMP_NS")
		case "array":
			if i+1 < len(ts) && ts[i+1] == "(" {
				r = append(r, "list_value")
			} else {
				r = append(r, s)
			}
		default:
			if strings.HasPrefix(s, "`") {
				r = append(r, Quote(strings.Trim(s, "`")))
			} else {
				r = append(r, s)
			}
		}
	}
	return strings.Join(r, " ")
}

func nameAt(ts []string, pos int) (string, int, error) {
	if pos >= len(ts) {
		return "", pos, fmt.Errorf("missing table name")
	}
	n := word(ts[pos])
	pos++
	if pos < len(ts) && ts[pos] == "." {
		return "", pos, fmt.Errorf("qualified SQL table names unsupported; select project/schema on SDK")
	}
	return n, pos, nil
}
func parseColumns(ts []string, pos int) ([]Column, int, error) {
	if pos >= len(ts) || ts[pos] != "(" {
		return nil, pos, fmt.Errorf("expected columns")
	}
	pos++
	var cols []Column
	for pos < len(ts) && ts[pos] != ")" {
		name := word(ts[pos])
		pos++
		start, depth := pos, 0
		for pos < len(ts) {
			s := ts[pos]
			if depth == 0 && (s == "," || s == ")" || word(s) == "comment" || word(s) == "not") {
				break
			}
			if s == "(" || s == "<" {
				depth++
			}
			if s == ")" || s == ">" {
				depth--
			}
			pos++
		}
		c, e := NewColumn(name, strings.Join(ts[start:pos], ""))
		if e != nil {
			return nil, pos, e
		}
		if pos < len(ts) && word(ts[pos]) == "comment" {
			pos += 2
		}
		if pos+1 < len(ts) && word(ts[pos]) == "not" && word(ts[pos+1]) == "null" {
			c.Nullable = false
			pos += 2
		}
		cols = append(cols, c)
		if pos < len(ts) && ts[pos] == "," {
			pos++
		} else {
			break
		}
	}
	if pos >= len(ts) || ts[pos] != ")" {
		return nil, pos, fmt.Errorf("invalid columns")
	}
	seen := map[string]bool{}
	for _, c := range cols {
		if seen[c.Name] {
			return nil, pos, fmt.Errorf("duplicate column %s", c.Name)
		}
		seen[c.Name] = true
	}
	return cols, pos + 1, nil
}
func parsePartition(ts []string, pos int) (map[string]string, int, error) {
	out := map[string]string{}
	if pos >= len(ts) || ts[pos] != "(" {
		return nil, pos, fmt.Errorf("expected partition spec")
	}
	pos++
	for pos < len(ts) && ts[pos] != ")" {
		if pos+2 >= len(ts) || ts[pos+1] != "=" {
			return nil, pos, fmt.Errorf("only static partition supported")
		}
		k := word(ts[pos])
		v := ts[pos+2]
		if !strings.HasPrefix(v, "'") || !strings.HasSuffix(v, "'") {
			return nil, pos, fmt.Errorf("partition value must be quoted")
		}
		if _, ok := out[k]; ok {
			return nil, pos, fmt.Errorf("duplicate partition key")
		}
		out[k] = strings.ReplaceAll(v[1:len(v)-1], "''", "'")
		pos += 3
		if pos < len(ts) && ts[pos] == "," {
			pos++
		} else {
			break
		}
	}
	if pos >= len(ts) || ts[pos] != ")" {
		return nil, pos, fmt.Errorf("invalid partition")
	}
	return out, pos + 1, nil
}
