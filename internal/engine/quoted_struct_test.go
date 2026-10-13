package engine

import "testing"

func TestQuotedStructDDLHasCanonicalSchema(t *testing.T) {
	c, err := NewColumn("v", "STRUCT<`x`:BIGINT,`y`:ARRAY<STRUCT<`z`:STRING>>>")
	if err != nil {
		t.Fatal(err)
	}
	if c.Type != "struct<x:bigint,y:array<struct<z:string>>>" {
		t.Fatal(c.Type)
	}
	if c.Parsed.Fields[0] != "x" || c.Parsed.Fields[1] != "y" || c.Parsed.Children[1].Children[0].Fields[0] != "z" {
		t.Fatal(c.Parsed)
	}
}
