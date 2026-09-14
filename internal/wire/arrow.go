package wire

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func appendValue(b array.Builder, t engine.Type, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch x := b.(type) {
	case *array.Int64Builder:
		x.Append(Int(v))
	case *array.Int32Builder:
		x.Append(int32(Int(v)))
	case *array.Int16Builder:
		x.Append(int16(Int(v)))
	case *array.Int8Builder:
		x.Append(int8(Int(v)))
	case *array.Float32Builder:
		x.Append(float32(Float(v)))
	case *array.Float64Builder:
		x.Append(Float(v))
	case *array.BooleanBuilder:
		x.Append(v.(bool))
	case *array.BinaryBuilder:
		x.Append(Bytes(v))
	case *array.Date32Builder:
		x.Append(arrow.Date32(v.(time.Time).Unix() / 86400))
	case *array.TimestampBuilder:
		tm := v.(time.Time)
		if t.Name == "datetime" {
			x.Append(arrow.Timestamp(tm.UnixMilli()))
		} else {
			x.Append(arrow.Timestamp(tm.UnixNano()))
		}
	case *array.Decimal128Builder:
		n, err := decimal128.FromString(fmt.Sprint(v), int32(t.Precision), int32(t.Scale))
		if err != nil {
			return err
		}
		x.Append(n)
	case *array.ListBuilder:
		x.Append(true)
		a := reflect.ValueOf(v)
		for i := 0; i < a.Len(); i++ {
			if err := appendValue(x.ValueBuilder(), t.Children[0], a.Index(i).Interface()); err != nil {
				return err
			}
		}
	case *array.MapBuilder:
		x.Append(true)
		m := reflect.ValueOf(v)
		for _, k := range Keys(v) {
			if err := appendValue(x.KeyBuilder(), t.Children[0], k.Interface()); err != nil {
				return err
			}
			if err := appendValue(x.ItemBuilder(), t.Children[1], m.MapIndex(k).Interface()); err != nil {
				return err
			}
		}
	case *array.StructBuilder:
		x.Append(true)
		m := v.(map[string]any)
		for i, n := range t.Fields {
			if err := appendValue(x.FieldBuilder(i), t.Children[i], m[n]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported Arrow builder %T", b)
	}
	return nil
}
func Arrow(r engine.Result, batchRows int, tunnel bool) (out []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			out = nil
			err = fmt.Errorf("Arrow conversion: %v", x)
		}
	}()
	fields := []arrow.Field{}
	for _, c := range r.Columns {
		fields = append(fields, arrow.Field{Name: c.Name, Type: c.Parsed.Arrow(), Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)
	var buf bytes.Buffer
	var stream *ipc.Writer
	if !tunnel {
		stream = ipc.NewWriter(&buf, ipc.WithSchema(schema))
	}
	if batchRows < 1 {
		batchRows = 4096
	}
	for start := 0; start < len(r.Rows); start += batchRows {
		end := start + batchRows
		if end > len(r.Rows) {
			end = len(r.Rows)
		}
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for _, row := range r.Rows[start:end] {
			for i, v := range row {
				if err = appendValue(b.Field(i), r.Columns[i].Parsed, v); err != nil {
					b.Release()
					return nil, err
				}
			}
		}
		rec := b.NewRecordBatch()
		b.Release()
		if tunnel {
			payload, e := ipc.GetRecordBatchPayload(rec)
			if e == nil {
				_, e = payload.WritePayload(&buf)
				payload.Release()
			}
			err = e
		} else {
			err = stream.Write(rec)
		}
		rec.Release()
		if err != nil {
			return nil, err
		}
		if buf.Len() > 64<<20 {
			return nil, fmt.Errorf("ResourceLimit: Arrow response exceeds 64 MiB")
		}
	}
	if tunnel {
		return ArrowChunk(buf.Bytes()), nil
	}
	if err = stream.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
