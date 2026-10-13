package server

import (
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"testing"
)

func TestCharacterWriteSchemaTypeCodesAndLength(t *testing.T) {
	for _, x := range []struct {
		typ  string
		code int
	}{{"char(4)", 9}, {"varchar(4)", 10}} {
		typ, err := engine.ParseType(x.typ)
		if err != nil {
			t.Fatal(err)
		}
		id := 0
		field := storageType("c", typ, true, &id)
		if field["Type"] != x.code || field["SpecifiedLength"] != 4 {
			t.Fatalf("%#v", field)
		}
	}
}
