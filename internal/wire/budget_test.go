package wire

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func TestProtobufDecodedNullRowsRespectBudget(t *testing.T) {
	cols := make([]engine.Column, 128)
	for i := range cols {
		cols[i], _ = engine.NewColumn("c", "bigint")
	}
	rows := make([][]any, 11000)
	for i := range rows {
		rows[i] = make([]any, len(cols))
	}
	payload, err := Protobuf(engine.Result{Columns: cols, Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > 100000 {
		t.Fatal("test requires a small payload")
	}
	_, err = DecodeProtobuf(payload, cols)
	if err == nil || !strings.Contains(err.Error(), "decoded records exceed") {
		t.Fatalf("NULL slots bypassed decoded budget: %v", err)
	}
}

func TestCompressedArrowAccountsDecodedValues(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	value := strings.Repeat("compressible", 10000)
	builder.Field(0).(*array.StringBuilder).Append(value)
	record := builder.NewRecordBatch()
	defer record.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema), ipc.WithZstd())
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	col, _ := engine.NewColumn("s", "string")
	result, err := DecodeArrow(buffer.Bytes(), []engine.Column{col}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes < int64(len(value)) || result.Rows[0][0] != value || buffer.Len() >= len(value) {
		t.Fatalf("compressed=%d retained=%d", buffer.Len(), result.Bytes)
	}
}

func TestArrowAllocationRejectedBeforeExpansion(t *testing.T) {
	allocator := &budgetAllocator{}
	defer func() {
		if recover() == nil {
			t.Fatal("oversized IPC buffer was accepted")
		}
	}()
	allocator.Allocate(MaxPayload + 1)
}
