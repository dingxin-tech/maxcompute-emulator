package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/antlr4-go/antlr/v4"
	parser "github.com/dingxin-tech/maxcompute-emulator/internal/sqlparser"
)

func TestBlockCommentsPreserveLiteralsAndHintTokens(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, sql := range []string{
		"/**/ select '/*literal*/' as v",
		"/* header; 'quoted' */ select /* inner */ '/*literal*/' as v /* tail */",
		"/*\n multiline\n*/ select '/*literal*/' as v",
	} {
		r, err := e.Execute(context.Background(), "p", "default", sql)
		if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != "/*literal*/" {
			t.Fatalf("comment/literal contract: %+v %v", r, err)
		}
	}
	for _, hint := range []string{"/*+ mapjoin(t) */", "/* + mapjoin(t) */"} {
		lexer := parser.NewOdpsLexer(antlr.NewInputStream(hint))
		tokens := lexer.GetAllTokens()
		if len(tokens) == 0 || tokens[0].GetTokenType() != parser.OdpsLexerHintStart {
			t.Fatalf("hint was swallowed: %s", hint)
		}
	}
	if _, err := Lex("/* unfinished select 1"); err == nil {
		t.Fatal("unterminated comment accepted")
	}
}

func TestLongBlockCommentWithinSQLLimit(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	sql := "/*" + strings.Repeat("x", (1<<20)-64) + "*/ select 73 as v"
	r, err := e.Execute(context.Background(), "p", "default", sql)
	if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != int32(73) {
		t.Fatalf("long comment: %+v %v", r, err)
	}
	if _, err := Lex("select 1 /*" + strings.Repeat("x", 1<<20) + "*/"); err == nil {
		t.Fatal("SQL byte limit ignored")
	}
}

func BenchmarkLexLongINList(b *testing.B) {
	for _, terms := range []int{32768, 131072} {
		b.Run(fmt.Sprint(terms), func(b *testing.B) {
			sql := "select ? as v where 1 in (1" + strings.Repeat(",1", terms-1) + ")"
			// Lex validates SQL before the JDBC placeholder has been substituted; use a
			// numeric literal here, as submitted to the emulator by the packaged driver.
			sql = strings.Replace(sql, "?", "73", 1)
			b.SetBytes(int64(len(sql)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Lex(sql); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestConcurrentTokenHeavySQL(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sql := "select 73 as v where 1 in (1" + strings.Repeat(",1", 8192) + ")"
	start := make(chan struct{})
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			<-start
			r, err := e.Execute(ctx, "p", "default", sql)
			if err == nil && (len(r.Rows) != 1 || fmt.Sprint(r.Rows[0][0]) != "73") {
				err = fmt.Errorf("incorrect result: %+v", r)
			}
			results <- err
		}()
	}
	close(start)
	for i := 0; i < 8; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestResultFormatComesFromExecutedStatement(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, tc := range []struct {
		sql string
		raw bool
	}{
		{"show schemas", true},
		{"show schemas; select 'schemas' as v", false},
		{"select 1; /*name listing*/ show schemas", true},
	} {
		r, err := e.Execute(context.Background(), "p", "default", tc.sql)
		if err != nil || r.RawNameList != tc.raw {
			t.Fatalf("result format %q: %+v %v", tc.sql, r, err)
		}
	}
}
