package wire

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

// DecodeArrow accepts standard IPC (Storage) or schema-less Tunnel chunks.
func DecodeArrow(b []byte, cols []engine.Column, tunnel bool) (result engine.Result, err error) {
	defer func() {
		if x := recover(); x != nil {
			result = engine.Result{}
			err = fmt.Errorf("invalid Arrow: %v", x)
		}
	}()
	result.Columns = cols
	if len(b) > MaxPayload {
		return result, fmt.Errorf("payload too large")
	}
	if tunnel {
		if len(b) == 0 {
			return result, nil
		} // Java closes an unused Arrow writer without emitting a frame.
		b, err = Unchunk(b)
		if err != nil {
			return result, err
		}
		if len(b) == 0 {
			return result, nil
		}
		prefix, e := Arrow(engine.Result{Columns: cols}, 1, false)
		if e != nil {
			return result, e
		}
		prefix = prefix[:len(prefix)-8]
		prefix = append(prefix, b...)
		b = append(prefix, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0)
	}
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return result, err
	}
	defer r.Release()
	if r.Schema().NumFields() != len(cols) {
		return result, fmt.Errorf("Arrow schema column count mismatch")
	}
	if !tunnel {
		for i, c := range cols {
			field := r.Schema().Field(i)
			if field.Name != c.Name || !arrow.TypeEqual(field.Type, c.Parsed.StorageArrow()) {
				return result, fmt.Errorf("Arrow schema mismatch for %s: %s", c.Name, field.Type)
			}
		}
	}
	for r.Next() {
		batch := r.RecordBatch()
		for i := int64(0); i < batch.NumRows(); i++ {
			row := make([]any, len(cols))
			for j, c := range cols {
				row[j], err = arrowValue(batch.Column(j), int(i), c.Parsed)
				if err != nil {
					return result, err
				}
			}
			result.Rows = append(result.Rows, row)
			if len(result.Rows) > 1000000 {
				return result, fmt.Errorf("too many records")
			}
		}
	}
	result.Bytes = int64(len(b))
	return result, r.Err()
}
func arrowValue(a arrow.Array, i int, t engine.Type) (any, error) {
	if a.IsNull(i) {
		return nil, nil
	}
	switch x := a.(type) {
	case *array.Int64:
		return x.Value(i), nil
	case *array.Int32:
		return int64(x.Value(i)), nil
	case *array.Int16:
		return int64(x.Value(i)), nil
	case *array.Int8:
		return int64(x.Value(i)), nil
	case *array.Float32:
		return x.Value(i), nil
	case *array.Float64:
		return x.Value(i), nil
	case *array.Boolean:
		return x.Value(i), nil
	case *array.String:
		return x.Value(i), nil
	case *array.Binary:
		if t.Name == "string" {
			return string(x.Value(i)), nil
		}
		return append([]byte(nil), x.Value(i)...), nil
	case *array.Decimal128:
		return x.Value(i).ToString(int32(t.Scale)), nil
	case *array.Date32:
		return time.Unix(int64(x.Value(i))*86400, 0).UTC(), nil
	case *array.Timestamp:
		n := int64(x.Value(i))
		switch x.DataType().(*arrow.TimestampType).Unit {
		case arrow.Second:
			return time.Unix(n, 0).UTC(), nil
		case arrow.Millisecond:
			return time.UnixMilli(n).UTC(), nil
		case arrow.Microsecond:
			return time.UnixMicro(n).UTC(), nil
		default:
			return time.Unix(0, n).UTC(), nil
		}
	case *array.List:
		start, end := x.ValueOffsets(i)
		v := []any{}
		for k := start; k < end; k++ {
			z, e := arrowValue(x.ListValues(), int(k), t.Children[0])
			if e != nil {
				return nil, e
			}
			v = append(v, z)
		}
		return v, nil
	case *array.Map:
		start, end := x.ValueOffsets(i)
		v := map[any]any{}
		for k := start; k < end; k++ {
			key, e := arrowValue(x.Keys(), int(k), t.Children[0])
			if e != nil {
				return nil, e
			}
			val, e := arrowValue(x.Items(), int(k), t.Children[1])
			if e != nil {
				return nil, e
			}
			v[key] = val
		}
		return v, nil
	case *array.Struct:
		v := map[string]any{}
		for j, n := range t.Fields {
			z, e := arrowValue(x.Field(j), i, t.Children[j])
			if e != nil {
				return nil, e
			}
			v[n] = z
		}
		return v, nil
	}
	return nil, fmt.Errorf("unsupported Arrow type %s", a.DataType())
}
