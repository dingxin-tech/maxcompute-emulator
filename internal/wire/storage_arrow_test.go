package wire

import (
	"bytes"
	"encoding/binary"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	flatbuffers "github.com/google/flatbuffers/go"
	"testing"
	"time"
)

func TestStorageArrowNegotiatedUnitsAndCompression(t *testing.T) {
	ts, _ := engine.NewColumn("ts", "timestamp")
	dt, _ := engine.NewColumn("dt", "datetime")
	nested, _ := engine.NewColumn("nested", "array<timestamp>")
	value := time.Date(2026, 10, 12, 1, 2, 3, 456789123, time.UTC)
	result := engine.Result{Columns: []engine.Column{ts, dt, nested}, Rows: [][]any{{value, value, []any{value}}, {nil, nil, nil}}}
	for unit, spec := range map[string]struct {
		unit  arrow.TimeUnit
		value int64
	}{
		"second": {arrow.Second, value.Unix()}, "milli": {arrow.Millisecond, value.UnixMilli()},
		"micro": {arrow.Microsecond, value.UnixMicro()}, "nano": {arrow.Nanosecond, value.UnixNano()},
	} {
		for _, compression := range []string{"", "ZSTD", "LZ4_FRAME"} {
			t.Run(unit+"/"+compression, func(t *testing.T) {
				data, err := StorageArrow(result, 1, compression, unit, unit)
				if err != nil {
					t.Fatal(err)
				}
				assertStorageCompression(t, data, compression)
				reader, err := ipc.NewReader(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Release()
				if !reader.Next() {
					t.Fatal(reader.Err())
				}
				record := reader.RecordBatch()
				for _, index := range []int{0, 1} {
					column := record.Column(index).(*array.Timestamp)
					if column.DataType().(*arrow.TimestampType).Unit != spec.unit || int64(column.Value(0)) != spec.value {
						t.Fatalf("unit/value: %v %v", column.DataType(), column.Value(0))
					}
				}
				child := record.Column(2).(*array.List).ListValues().(*array.Timestamp)
				if child.DataType().(*arrow.TimestampType).Unit != spec.unit || int64(child.Value(0)) != spec.value {
					t.Fatal("nested timestamp precision")
				}
				if !reader.Next() || !reader.RecordBatch().Column(0).IsNull(0) {
					t.Fatal("null batch missing")
				}
				if reader.Next() || reader.Err() != nil {
					t.Fatal("unexpected batch", reader.Err())
				}
			})
		}
	}
}

// Inspect the public Arrow IPC metadata, so ignored compression cannot pass a round trip.
func assertStorageCompression(t *testing.T, stream []byte, expected string) {
	t.Helper()
	for len(stream) >= 8 {
		size := int(binary.LittleEndian.Uint32(stream[4:8]))
		if size == 0 {
			break
		}
		if len(stream) < 8+size {
			t.Fatal("truncated IPC metadata")
		}
		metadata := stream[8 : 8+size]
		message := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.GetUOffsetT(metadata)}
		kind := message.GetByteSlot(6, 0)
		if kind == 3 {
			var batch flatbuffers.Table
			message.Union(&batch, flatbuffers.UOffsetT(message.Offset(8)))
			offset := flatbuffers.UOffsetT(batch.Offset(10))
			if expected == "" {
				if offset != 0 {
					t.Fatal("unexpected compressed batch")
				}
				return
			}
			if offset == 0 {
				t.Fatal("requested compression missing")
			}
			compression := flatbuffers.Table{Bytes: metadata, Pos: batch.Indirect(batch.Pos + offset)}
			codec := compression.GetByteSlot(4, 0)
			want := byte(0)
			if expected == "ZSTD" {
				want = 1
			}
			if codec != want {
				t.Fatalf("compression codec %d, want %d", codec, want)
			}
			return
		}
		body := message.GetInt64Slot(10, 0)
		stream = stream[8+size+int(body):]
	}
	t.Fatal("record batch metadata missing")
}
