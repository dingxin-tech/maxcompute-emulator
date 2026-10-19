package engine

import (
	"context"
	"strings"
	"testing"

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
