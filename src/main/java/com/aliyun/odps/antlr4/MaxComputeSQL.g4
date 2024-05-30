grammar MaxComputeSQL;

@header {
package com.aliyun.odps.antlr4.generate;
}

sqlStatement
    : createTable EOF
    ;

createTable
    : 'CREATE' 'TABLE' ifNotExists? tableName '(' columnDef (',' columnDef)* (',' primaryKey)? ')' partitionedBy? tblProperties? ';'
    ;

ifNotExists
    : 'IF' 'NOT' 'EXISTS'
    ;

tableName
    : IDENTIFIER ('.' IDENTIFIER)*
    ;

columnDef
    : quotedIdentifier dataType notNull? comment?
    ;

comment
    : 'COMMENT' quotedIdentifier
    ;

primaryKey
    : 'PRIMARY' 'KEY' '(' quotedIdentifier (',' quotedIdentifier)* ')'
    ;

partitionedBy
    : 'PARTITIONED' 'BY' '(' partitionColumnDef (',' partitionColumnDef)* ')'
    ;

partitionColumnDef
    : quotedIdentifier dataType notNull? comment?
    ;

tblProperties
    : 'TBLPROPERTIES' '(' * ')'
    ;


dataType
    : 'BIGINT' | 'STRING' | 'INT' | 'DOUBLE' | 'FLOAT' | 'BOOLEAN'
    ;

notNull
    : 'NOT' 'NULL'
    ;

quotedIdentifier
    : '`' IDENTIFIER '`' | '\'' IDENTIFIER '\'' | IDENTIFIER
    ;


IDENTIFIER
    : [a-zA-Z_][a-zA-Z_0-9]*
    ;

WS
    : [ \t\r\n]+ -> skip
    ;
