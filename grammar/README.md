# ODPS grammar provenance

The unmodified public OdpsLexer.g4 and OdpsParser.g4 in upstream/ come from
aliyun/aliyun-odps-java-sdk commit 1c11317c18ad63ac84030867f5a8dd2def48122f,
odps-sdk/odps-sdk-core/src/main/java/com/aliyun/odps/sqa/commandapi/antlr/sql/.
Their Apache-2.0 notices are retained. No internal server grammar was copied.

scripts/adapt-grammar.py makes Go-target changes and explicit dialect extensions to temporary copies:
remove the Java context superclass, boolean argument to bool, rename Identifier
to avoid a Go member collision, emptyStatement/newExpression to avoid generated
Go constructor collisions, and separate the M/m month/minute labels.

ANTLR generator 4.13.2 and Go runtime 4.13.1 are used. Generated files are checked
into internal/sqlparser. Regenerate with:

```bash
ANTLR_JAR=/path/to/antlr-4.13.2-complete.jar scripts/generate-parser.sh
```

The adaptation also recognizes TIMESTAMP_NTZ as a timestamp token while retaining
its original token text for the engine type mapping. Upstream files stay unchanged.

The lexer MUST be generated first and the parser must use its matching tokens.
The parser validates syntax; internal/engine implements the supported execution
subset. Successful parsing alone does not promise ODPS semantic compatibility.
