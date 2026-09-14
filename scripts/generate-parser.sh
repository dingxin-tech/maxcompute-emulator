#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${ANTLR_JAR:?Set ANTLR_JAR to antlr-4.13.2-complete.jar}"
parser_tmp="$(mktemp -d)"
trap 'rm -rf "$parser_tmp"' EXIT
mkdir -p "$parser_tmp/grammar" "$parser_tmp/out"
cp grammar/upstream/*.g4 "$parser_tmp/grammar/"
python3 scripts/adapt-grammar.py "$parser_tmp/grammar"
(
 cd "$parser_tmp"
 java -jar "$ANTLR_JAR" -Dlanguage=Go -package parser -o out grammar/OdpsLexer.g4
 java -jar "$ANTLR_JAR" -Dlanguage=Go -package parser -lib out/grammar -o out grammar/OdpsParser.g4
)
cp "$parser_tmp/out/grammar/"*.go internal/sqlparser/
gofmt -w internal/sqlparser
