package wire

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

const MaxPayload = 64 << 20

type decoder struct {
	crc *encoder
	b   []byte
	pos int
}

func (d *decoder) take(n int) ([]byte, error) {
	if n < 0 || n > len(d.b)-d.pos {
		return nil, fmt.Errorf("truncated payload")
	}
	b := d.b[d.pos : d.pos+n]
	d.pos += n
	return b, nil
}
func (d *decoder) u() (uint64, error) {
	v, n := binary.Uvarint(d.b[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid varint")
	}
	d.pos += n
	return v, nil
}
func (d *decoder) s() (int64, error) { u, e := d.u(); return int64(u>>1) ^ -int64(u&1), e }
func (d *decoder) value(t engine.Type) (value any, err error) {
	defer func() {
		if err == nil && t.Name != "array" && t.Name != "map" && t.Name != "struct" {
			err = d.crc.value(t, value)
			d.crc.b = nil
		}
	}()

	switch t.Name {
	case "bigint", "int", "smallint", "tinyint":
		return d.s()
	case "boolean":
		n, e := d.u()
		if n > 1 {
			return nil, fmt.Errorf("invalid boolean")
		}
		return n == 1, e
	case "float":
		b, e := d.take(4)
		if e != nil {
			return nil, e
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(b)), nil
	case "double":
		b, e := d.take(8)
		if e != nil {
			return nil, e
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
	case "string", "binary", "decimal":
		n, e := d.u()
		if e != nil || n > MaxPayload {
			return nil, fmt.Errorf("invalid field size")
		}
		b, e := d.take(int(n))
		if e != nil {
			return nil, e
		}
		if t.Name == "binary" {
			return append([]byte(nil), b...), nil
		}
		return string(b), nil
	case "date", "datetime", "timestamp", "timestamp_ntz":
		n, e := d.s()
		if e != nil {
			return nil, e
		}
		if t.Name == "date" {
			return time.Unix(n*86400, 0).UTC(), nil
		}
		if t.Name == "datetime" {
			return time.UnixMilli(n).UTC(), nil
		}
		ns, e := d.s()
		if e != nil || ns < 0 || ns >= 1e9 {
			return nil, fmt.Errorf("invalid timestamp nanos")
		}
		return time.Unix(n, ns).UTC(), nil
	case "array":
		return d.array(t.Children[0])
	case "map":
		keys, e := d.array(t.Children[0])
		if e != nil {
			return nil, e
		}
		vals, e := d.array(t.Children[1])
		if e != nil {
			return nil, e
		}
		if len(keys) != len(vals) {
			return nil, fmt.Errorf("map lengths differ")
		}
		m := map[any]any{}
		for i, k := range keys {
			if k == nil {
				return nil, fmt.Errorf("NULL map key")
			}
			if _, exists := m[k]; exists {
				return nil, fmt.Errorf("duplicate map key")
			}
			m[k] = vals[i]
		}
		return m, nil
	case "struct":
		m := map[string]any{}
		for i, n := range t.Fields {
			null, e := d.u()
			if e != nil || null > 1 {
				return nil, fmt.Errorf("invalid struct null flag")
			}
			if null == 1 {
				m[n] = nil
				continue
			}
			v, e := d.value(t.Children[i])
			if e != nil {
				return nil, e
			}
			m[n] = v
		}
		return m, nil
	}
	return nil, fmt.Errorf("unsupported type %s", t.Name)
}
func (d *decoder) array(t engine.Type) ([]any, error) {
	n, e := d.u()
	if e != nil || n > 100000 {
		return nil, fmt.Errorf("invalid collection size")
	}
	out := make([]any, int(n))
	for i := range out {
		null, e := d.u()
		if e != nil || null > 1 {
			return nil, fmt.Errorf("invalid null flag")
		}
		if null == 0 {
			out[i], e = d.value(t)
			if e != nil {
				return nil, e
			}
		}
	}
	return out, nil
}

// DecodeProtobuf validates the complete record stream before any rows are visible.
func DecodeProtobuf(b []byte, cols []engine.Column) (result engine.Result, err error) {
	defer func() {
		if x := recover(); x != nil {
			result = engine.Result{}
			err = fmt.Errorf("invalid protobuf value: %v", x)
		}
	}()
	if len(b) > MaxPayload {
		return result, fmt.Errorf("payload too large")
	}
	d := decoder{b: b}
	result.Columns = cols
	row := make([]any, len(cols))
	seen := map[int]bool{}
	crc := encoder{}
	d.crc = &crc
	global := uint32(0)
	for d.pos < len(b) {
		tag, e := d.u()
		if e != nil {
			return result, e
		}
		field := int(tag >> 3)
		switch field {
		case 33553408:
			sum, e := d.u()
			if e != nil || sum > math.MaxUint32 || uint32(sum) != crc.crc {
				return result, fmt.Errorf("record CRC32C mismatch")
			}
			global = crc32.Update(global, castagnoli, binary.LittleEndian.AppendUint32(nil, uint32(sum)))
			result.Rows = append(result.Rows, row)
			result.Bytes += retainedBytes(row)
			if result.Bytes > MaxPayload {
				return result, fmt.Errorf("decoded records exceed 64 MiB")
			}
			if len(result.Rows) > 1000000 {
				return result, fmt.Errorf("too many records")
			}
			row = make([]any, len(cols))
			seen = map[int]bool{}
			crc = encoder{}
		case 33554430:
			if len(seen) != 0 {
				return result, fmt.Errorf("unterminated record")
			}
			count, e := d.s()
			if e != nil || count != int64(len(result.Rows)) {
				return result, fmt.Errorf("record count mismatch")
			}
			tag, e = d.u()
			if e != nil || tag != 33554431<<3 {
				return result, fmt.Errorf("missing stream checksum")
			}
			sum, e := d.u()
			if e != nil || sum > math.MaxUint32 || uint32(sum) != global || d.pos != len(b) {
				return result, fmt.Errorf("stream CRC32C mismatch or trailing data")
			}
			return result, nil
		default:
			i := field - 1
			if i < 0 || i >= len(cols) || seen[i] || tag&7 != wireType(cols[i].Parsed) {
				return result, fmt.Errorf("invalid field tag %d", tag)
			}
			seen[i] = true
			crc.i32(uint32(field))
			v, e := d.value(cols[i].Parsed)
			if e != nil {
				return result, e
			}
			row[i] = v
		}
	}
	return result, fmt.Errorf("missing stream footer")
}

func Unchunk(b []byte) ([]byte, error) {
	if len(b) == 4 && binary.BigEndian.Uint32(b) == 0 {
		return nil, nil
	} // empty Java writer
	if len(b) < 8 || len(b) > MaxPayload {
		return nil, fmt.Errorf("invalid Arrow frame")
	}
	size := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	if size <= 0 || size > MaxPayload {
		return nil, fmt.Errorf("invalid chunk size")
	}
	out := []byte{}
	for len(b) > size+4 {
		chunk := b[:size]
		if crc32.Checksum(chunk, castagnoli) != binary.BigEndian.Uint32(b[size:]) {
			return nil, fmt.Errorf("Arrow chunk CRC32C mismatch")
		}
		out = append(out, chunk...)
		b = b[size+4:]
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("missing Arrow checksum")
	}
	out = append(out, b[:len(b)-4]...)
	if crc32.Checksum(out, castagnoli) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("Arrow stream CRC32C mismatch")
	}
	return out, nil
}
