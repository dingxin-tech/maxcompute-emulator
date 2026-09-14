package engine

import "fmt"

// extractPrimaryKey removes the ODPS NOT ENFORCED declaration from the physical
// DDL; the logical key is used by the Upsert transaction implementation.
func extractPrimaryKey(ts []string) ([]string, []string, error) {
	keys := []string{}
	for i := 0; i+2 < len(ts); i++ {
		if word(ts[i]) != "primary" || word(ts[i+1]) != "key" {
			continue
		}
		if ts[i+2] != "(" {
			return nil, nil, fmt.Errorf("invalid primary key")
		}
		end := i + 3
		for end < len(ts) && ts[end] != ")" {
			if ts[end] != "," {
				keys = append(keys, word(ts[end]))
			}
			end++
		}
		if end >= len(ts) || len(keys) == 0 {
			return nil, nil, fmt.Errorf("invalid primary key")
		}
		end++
		if end+1 < len(ts) && word(ts[end]) == "not" && word(ts[end+1]) == "enforced" {
			end += 2
		}
		start := i
		if start > 0 && ts[start-1] == "," {
			start--
		} else if end < len(ts) && ts[end] == "," {
			end++
		}
		out := append([]string{}, ts[:start]...)
		out = append(out, ts[end:]...)
		return out, keys, nil
	}
	return ts, keys, nil
}
