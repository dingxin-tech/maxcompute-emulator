package engine

import (
	"context"
	"fmt"
	"testing"
)

func TestODPSTypedNumericLiterals(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, tc := range []struct{ sql, typ, value string }{
		{"73L", "bigint", "73"},
		{"9223372036854775807l", "bigint", "9223372036854775807"},
		{"-9223372036854775808L", "bigint", "-9223372036854775808"},
		{"127Y", "tinyint", "127"}, {"-128y", "tinyint", "-128"},
		{"32767S", "smallint", "32767"}, {"-32768s", "smallint", "-32768"},
		{"10BD", "decimal(2,0)", "10"},
		{"0.00120bd", "decimal(5,5)", "0.00120"},
		{"1.25E2BD", "decimal(3,0)", "125"},
		{"1.25f", "float", "1.25"}, {"-1.25E2F", "float", "-125.0"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			r, err := e.Execute(context.Background(), "p", "default", "select "+tc.sql+" as n, cast("+tc.sql+" as string) as s")
			if err != nil {
				t.Fatal(err)
			}
			if r.Columns[0].Type != tc.typ || len(r.Rows) != 1 || r.Rows[0][1] != tc.value {
				t.Fatalf("want %s %s, got %+v", tc.typ, tc.value, r)
			}
		})
	}
	for _, literal := range []string{"128Y", "-129Y", "32768S", "-32769S", "9223372036854775808L"} {
		if _, err := e.Execute(context.Background(), "p", "default", "select "+literal); err == nil {
			t.Errorf("out-of-range literal accepted: %s", literal)
		}
	}
	for _, query := range []string{
		"select 1 where -128Y = -128Y",
		"select 1 having -32768S = -32768S",
		"select case -9223372036854775808L when -9223372036854775808L then 1 else 0 end",
		"select cast(not -128Y = 0Y as int)",
	} {
		r, err := e.Execute(context.Background(), "p", "default", query)
		if err != nil || len(r.Rows) != 1 || fmt.Sprint(r.Rows[0][0]) != "1" {
			t.Fatalf("signed minimum in expression: %s: %+v %v", query, r, err)
		}
	}
	r, err := e.Execute(context.Background(), "p", "default", "select '73L' as `10BD`, 5L - 2L as n, -(1L) as m")
	if err != nil || r.Columns[0].Name != "10BD" || r.Rows[0][0] != "73L" || r.Rows[0][1] != int64(3) || r.Rows[0][2] != int64(-1) {
		t.Fatalf("literal translation changed a string/identifier/operator: %+v %v", r, err)
	}
}
