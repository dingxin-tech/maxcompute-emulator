package wire

import (
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func TestRejectTruncatedAndCorruptPayloads(t *testing.T) {
	c, _ := engine.NewColumn("id", "bigint")
	cols := []engine.Column{c}
	data := engine.Result{Columns: cols, Rows: [][]any{{int64(123)}}}
	b, e := Protobuf(data)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < len(b); i++ {
		if _, e := DecodeProtobuf(b[:i], cols); e == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	for i := range b {
		bad := append([]byte{}, b...)
		bad[i] ^= 0x80
		if _, e := DecodeProtobuf(bad, cols); e == nil {
			t.Fatalf("accepted corruption at %d", i)
		}
	}
	a, e := Arrow(data, 1, true)
	if e != nil {
		t.Fatal(e)
	}
	a[len(a)-1] ^= 1
	if _, e := DecodeArrow(a, cols, true); e == nil {
		t.Fatal("accepted Arrow CRC corruption")
	}
	other, _ := engine.NewColumn("id", "string")
	a, _ = Arrow(engine.Result{Columns: []engine.Column{other}, Rows: [][]any{{"123"}}}, 1, false)
	if _, e := DecodeArrow(a, cols, false); e == nil {
		t.Fatal("accepted wrong Arrow schema")
	}
}
