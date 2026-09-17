"""Adapt pinned public grammar copies for ANTLR Go. Only writes provided temp dir."""
from pathlib import Path
import re, sys
base = Path(sys.argv[1])
for name in ('OdpsLexer', 'OdpsParser'):
    p = base / (name + '.g4')
    text = p.read_text()
    text = re.sub(r'\bIdentifier\b', 'IDENTIFIER_TOKEN', text)
    if name == 'OdpsLexer':
        text = text.replace('KW_TIMESTAMP: T I M E S T A M P;', "KW_TIMESTAMP: T I M E S T A M P ('_' N T Z)?;")
    if name == 'OdpsParser':
        text = text.replace('    contextSuperClass=OdpsParserRuleContext;', '')
        text = text.replace('[boolean table]', '[bool table]')
        text = re.sub(r'\bemptyStatement\b', 'noopStatement', text)
        text = re.sub(r'\bnewExpression\b', 'constructExpression', text)
        text = text.replace('M=KW_MONTH', 'monthUnit=KW_MONTH')
        text = text.replace('m=KW_MINUTE', 'minuteUnit=KW_MINUTE')
    p.write_text(text)
