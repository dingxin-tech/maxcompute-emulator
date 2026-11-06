package server

import "github.com/dingxin-tech/maxcompute-emulator/internal/engine"

func readSchema(cols []engine.Column) map[string]any {
	data := []any{}
	for i, c := range cols {
		data = append(data, map[string]any{"Name": c.Name, "Type": c.Type, "Nullable": c.Nullable, "ColumnId": i, "Comment": c.Comment})
	}
	return map[string]any{"DataColumns": data, "PartitionColumns": []any{}, "SystemColumns": []any{}, "BlobColumns": []any{}}
}
func writeSchema(cols []engine.Column) map[string]any {
	data := []any{}
	n := 0
	for _, c := range cols {
		data = append(data, map[string]any{"columnType": storageType(c.Name, c.Parsed, c.Nullable, &n), "comment": c.Comment, "label": "", "extendedLabels": []any{}})
	}
	return map[string]any{"DataColumns": data, "PartitionColumns": []any{}, "SystemColumns": []any{}}
}
func storageType(name string, t engine.Type, nullable bool, n *int) map[string]any {
	codes := map[string]int{"bigint": 0, "double": 1, "boolean": 2, "datetime": 3, "string": 4, "decimal": 5, "tinyint": 6, "smallint": 7, "int": 8, "char": 9, "varchar": 10, "binary": 11, "date": 12, "timestamp": 13, "float": 14, "array": 17, "map": 18, "struct": 19, "timestamp_ntz": 21}
	out := map[string]any{"MemberName": name, "ColumnId": *n, "Type": codes[t.Name], "Nullable": nullable, "Precision": t.Precision, "Scale": t.Scale}
	*n++
	if t.Name == "char" || t.Name == "varchar" {
		out["SpecifiedLength"] = t.Precision
	}
	children := []any{}
	for i, c := range t.Children {
		name := "element"
		if t.Name == "map" {
			name = []string{"key", "value"}[i]
		}
		if t.Name == "struct" {
			name = t.Fields[i]
		}
		children = append(children, storageType(name, c, true, n))
	}
	out["SubTypes"] = children
	return out
}

// deltaSchema publishes the three buckets an incremental session reads back in the
// order consumers concatenate them: data columns, system columns, partition columns.
func deltaSchema(cols []engine.Column, partitions []engine.Column) map[string]any {
	names := map[string]bool{}
	for _, c := range partitions {
		names[c.Name] = true
	}
	data, system, part := []any{}, []any{}, []any{}
	id := 0
	add := func(bucket *[]any, c engine.Column) {
		*bucket = append(*bucket, map[string]any{"Name": c.Name, "Type": c.Type, "Nullable": c.Nullable, "ColumnId": id, "Comment": c.Comment})
		id++
	}
	for _, c := range cols {
		switch {
		case c.Name == engine.MetaOpColumn || c.Name == engine.MetaTSColumn:
			add(&system, c)
		case names[c.Name]:
			add(&part, c)
		default:
			add(&data, c)
		}
	}
	return map[string]any{"DataColumns": data, "PartitionColumns": part, "SystemColumns": system, "BlobColumns": []any{}}
}
