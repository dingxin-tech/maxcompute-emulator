package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Match complete lexer tokens only, never text inside strings or identifiers.
var integerLiteral = regexp.MustCompile(`(?i)^[+-]?[0-9]+([LSY])$`)
var decimalLiteral = regexp.MustCompile(`(?i)^[+-]?[0-9]+(?:\.[0-9]*)?(?:E[+-]?[0-9]+)?BD$`)
var floatLiteral = regexp.MustCompile(`(?i)^[+-]?[0-9]+(?:\.[0-9]*)?(?:E[+-]?[0-9]+)?F$`)

func typedLiteral(s string) (string, bool) {
	if floatLiteral.MatchString(s) {
		return "CAST('" + s[:len(s)-1] + "' AS FLOAT)", true
	}
	if m := integerLiteral.FindStringSubmatch(s); m != nil {
		typ := map[string]string{"L": "BIGINT", "S": "SMALLINT", "Y": "TINYINT"}[strings.ToUpper(m[1])]
		return "CAST('" + s[:len(s)-1] + "' AS " + typ + ")", true
	}
	if !decimalLiteral.MatchString(s) {
		return "", false
	}
	n := s[:len(s)-2]
	mantissa := strings.TrimLeft(n, "+-")
	exponent := 0
	if i := strings.IndexAny(mantissa, "eE"); i >= 0 {
		var err error
		exponent, err = strconv.Atoi(mantissa[i+1:])
		if err != nil || exponent > 1000 || exponent < -1000 {
			// An invalid target type makes extreme exponents fail without allocation.
			return "CAST('" + n + "' AS DECIMAL(39,0))", true
		}
		mantissa = mantissa[:i]
	}
	scale := -exponent
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		scale += len(mantissa) - i - 1
	}
	digits := strings.TrimLeft(strings.ReplaceAll(mantissa, ".", ""), "0")
	precision := len(digits)
	if precision == 0 {
		precision = 1
	}
	if scale < 0 {
		precision -= scale
		scale = 0
	}
	if precision < scale {
		precision = scale
	}
	return fmt.Sprintf("CAST('%s' AS DECIMAL(%d,%d))", n, precision, scale), true
}

func unaryPosition(ts []string, i int) bool {
	if i == 0 {
		return true
	}
	switch strings.ToLower(ts[i-1]) {
	case "(", ",", "+", "-", "*", "/", "%", "=", "<", ">", "<=", ">=", "<>", "!=", "&", "|", "^", "~", "!",
		"select", "distinct", "all", "where", "having", "on", "case", "when", "then", "else", "not", "and", "or", "between", "by", "in", "is":
		return true
	}
	return false
}
