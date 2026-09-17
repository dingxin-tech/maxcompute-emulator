// Tunnel wire behavior is independently implemented against the public ODPS
// Java/Go/C++ SDKs. See docs/protocol.md for pinned references.
package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type encoder struct {
	b   []byte
	crc uint32
}

func (e *encoder) u(v uint64)   { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) s(v int64)    { e.u(uint64(v<<1) ^ uint64(v>>63)) }
func (e *encoder) sum(b []byte) { e.crc = crc32.Update(e.crc, castagnoli, b) }
func (e *encoder) i32(v uint32) { e.sum(binary.LittleEndian.AppendUint32(nil, v)) }
func (e *encoder) i64(v uint64) { e.sum(binary.LittleEndian.AppendUint64(nil, v)) }
func Int(v any) int64           { return reflect.ValueOf(v).Int() }
func Float(v any) float64       { return reflect.ValueOf(v).Float() }
func Bytes(v any) []byte {
	if b, ok := v.([]byte); ok {
		return b
	}
	return []byte(fmt.Sprint(v))
}
func Keys(v any) []reflect.Value {
	ks := reflect.ValueOf(v).MapKeys()
	sort.Slice(ks, func(i, j int) bool { return fmt.Sprint(ks[i].Interface()) < fmt.Sprint(ks[j].Interface()) })
	return ks
}
func wireType(t engine.Type) uint64 {
	switch t.Name {
	case "bigint", "int", "smallint", "tinyint", "boolean", "datetime", "date":
		return 0
	case "double":
		return 1
	case "float":
		return 5
	default:
		return 2
	}
}
func (e *encoder) value(t engine.Type, v any) error {
	switch t.Name {
	case "bigint", "int", "smallint", "tinyint":
		n := Int(v)
		e.i64(uint64(n))
		e.s(n)
	case "float":
		bits := math.Float32bits(float32(Float(v)))
		e.i32(bits)
		e.b = binary.LittleEndian.AppendUint32(e.b, bits)
	case "double":
		bits := math.Float64bits(Float(v))
		e.i64(bits)
		e.b = binary.LittleEndian.AppendUint64(e.b, bits)
	case "boolean":
		var n byte
		if v.(bool) {
			n = 1
		}
		e.sum([]byte{n})
		e.u(uint64(n))
	case "string", "binary", "decimal":
		b := Bytes(v)
		e.sum(b)
		e.u(uint64(len(b)))
		e.b = append(e.b, b...)
	case "date", "datetime", "timestamp", "timestamp_ntz":
		tm := v.(time.Time)
		switch t.Name {
		case "date":
			n := tm.Unix() / 86400
			e.i64(uint64(n))
			e.s(n)
		case "datetime":
			n := tm.UnixMilli()
			e.i64(uint64(n))
			e.s(n)
		default:
			e.i64(uint64(tm.Unix()))
			e.i32(uint32(tm.Nanosecond()))
			e.s(tm.Unix())
			e.s(int64(tm.Nanosecond()))
		}
	case "array":
		a := reflect.ValueOf(v)
		e.u(uint64(a.Len()))
		for i := 0; i < a.Len(); i++ {
			x := a.Index(i).Interface()
			if x == nil {
				e.u(1)
			} else {
				e.u(0)
				if err := e.value(t.Children[0], x); err != nil {
					return err
				}
			}
		}
	case "map":
		m := reflect.ValueOf(v)
		ks := Keys(v)
		e.u(uint64(len(ks)))
		for _, k := range ks {
			e.u(0)
			if err := e.value(t.Children[0], k.Interface()); err != nil {
				return err
			}
		}
		e.u(uint64(len(ks)))
		for _, k := range ks {
			x := m.MapIndex(k).Interface()
			if x == nil {
				e.u(1)
			} else {
				e.u(0)
				if err := e.value(t.Children[1], x); err != nil {
					return err
				}
			}
		}
	case "struct":
		m := v.(map[string]any)
		for i, name := range t.Fields {
			x := m[name]
			if x == nil {
				e.u(1)
			} else {
				e.u(0)
				if err := e.value(t.Children[i], x); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("unsupported protobuf type %s", t.Name)
	}
	return nil
}
func Protobuf(r engine.Result) ([]byte, error) { return protobuf(r, true) }

// ProtobufPrefix omits the stream trailer for row-boundary disconnect tests.
func ProtobufPrefix(r engine.Result) ([]byte, error) { return protobuf(r, false) }
func protobuf(r engine.Result, trailer bool) (out []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			out = nil
			err = fmt.Errorf("type conversion: %v", x)
		}
	}()
	e := encoder{}
	global := uint32(0)
	for _, row := range r.Rows {
		e.crc = 0
		for i, v := range row {
			if v == nil {
				continue
			}
			e.i32(uint32(i + 1))
			e.u(uint64(i+1)<<3 | wireType(r.Columns[i].Parsed))
			if err = e.value(r.Columns[i].Parsed, v); err != nil {
				return nil, err
			}
		}
		e.u(33553408 << 3)
		e.u(uint64(e.crc))
		global = crc32.Update(global, castagnoli, binary.LittleEndian.AppendUint32(nil, e.crc))
		if len(e.b) > 64<<20 {
			return nil, fmt.Errorf("ResourceLimit: response exceeds 64 MiB")
		}
	}
	if !trailer {
		// A partial next tag forces transport truncation instead of a clean EOF.
		return append(e.b, 0x80), nil
	}
	e.u(33554430 << 3)
	e.s(int64(len(r.Rows)))
	e.u(33554431 << 3)
	e.u(uint64(global))
	return e.b, nil
}

// ArrowChunk wraps schema-less IPC record batches in Tunnel's big-endian
// chunk-size/checksum framing. Storage API uses ordinary IPC instead.
func ArrowChunk(data []byte) []byte {
	const size = 65536
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(size))
	global := crc32.Checksum(data, castagnoli)
	for len(data) >= size {
		b.Write(data[:size])
		binary.Write(&b, binary.BigEndian, crc32.Checksum(data[:size], castagnoli))
		data = data[size:]
	}
	b.Write(data)
	binary.Write(&b, binary.BigEndian, global)
	return b.Bytes()
}
