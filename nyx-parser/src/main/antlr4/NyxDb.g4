// ANTLR4 grammar for common SQL DDL statements (CREATE/DROP/ALTER/INDEX)
grammar NyxDb;

@header {
    package org.nyxdb.parser.core;
}
ddlFile
    : statement* EOF
    ;

statement
    : ddlStatement (';' | EOF)
    | dmlStatement (';' | EOF)
    | SEMI
    ;

ddlStatement
    : createTableStmt
    | dropTableStmt
    | truncateTableStmt
    | dropDatabaseStmt
    | createDatabaseStmt
    | alterTableStmt
    | createIndexStmt
    | dropIndexStmt
    ;

dmlStatement
    : insertStmt
    ;

insertStmt
    : INSERT INTO qualifiedName (LPAREN columnList RPAREN)? VALUES LPAREN valueList RPAREN (COMMA LPAREN valueList RPAREN)*
    ;

valueList
    : literal (COMMA literal)*
    ;

createTableStmt
    : CREATE TABLE ifNotExists? qualifiedName LPAREN columnDef (COMMA columnDef)* (COMMA tableConstraint)* RPAREN
    ;

dropTableStmt
    : DROP TABLE ifExists? qualifiedName
    ;

truncateTableStmt
    : TRUNCATE TABLE ifExists? qualifiedName
    ;

alterTableStmt
    : ALTER TABLE qualifiedName alterAction (COMMA alterAction)*
    ;

alterAction
    : ADD (COLUMN)? columnDef
    | DROP (COLUMN)? identifier
    | ADD tableConstraint
    ;

createIndexStmt
    : CREATE (UNIQUE)? INDEX ifNotExists? identifier ON qualifiedName LPAREN columnList RPAREN
    ;

dropIndexStmt
    : DROP INDEX ifExists? identifier
    ;

createDatabaseStmt
    : CREATE DATABASE ifNotExists? identifier
    ;

dropDatabaseStmt
    : DROP DATABASE ifExists? identifier
    ;

ifNotExists
    : IF NOT EXISTS
    ;

ifExists
    : IF EXISTS
    ;

columnDef
    : identifier dataType columnConstraint*
    ;

tableConstraint
    : PRIMARY KEY LPAREN columnList RPAREN
    | UNIQUE LPAREN columnList RPAREN
    | FOREIGN KEY LPAREN columnList RPAREN REFERENCES qualifiedName LPAREN columnList RPAREN
    ;

columnConstraint
    : NOT NULL
    | NULL
    | PRIMARY KEY
    | UNIQUE
    | DEFAULT literal
    | CHECK LPAREN expression RPAREN
    ;

dataType
    : identifier (LPAREN numericLiteral (COMMA numericLiteral)? RPAREN)?
    ;

columnList
    : identifier (COMMA identifier)*
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

expression
    : <assoc=right> expression OR expression
    | expression AND expression
    | expression comparisonOperator expression
    | LPAREN expression RPAREN
    | predicate
    ;

predicate
    : identifier comparisonOperator literal
    | literal
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

literal
    : STRING_LITERAL
    | numericLiteral
    ;

numericLiteral
    : NUMERIC_LITERAL
    ;

identifier
    : IDENTIFIER
    ;

// -------------------- Lexer --------------------

CREATE: [cC][rR][eE][aA][tT][eE];
TABLE: [tT][aA][bB][lL][eE];
DROP: [dD][rR][oO][pP];
ALTER: [aA][lL][tT][eE][rR];
ADD: [aA][dD][dD];
COLUMN: [cC][oO][lL][uU][mM][nN];
IF: [iI][fF];
NOT: [nN][oO][tT];
EXISTS: [eE][xX][iI][sS][tT][sS];
PRIMARY: [pP][rR][iI][mM][aA][rR][yY];
KEY: [kK][eE][yY];
UNIQUE: [uU][nN][iI][qQ][uU][eE];
REFERENCES: [rR][eE][fF][eE][rR][eE][nN][cC][eE][sS];
FOREIGN: [fF][oO][rR][eE][iI][gG][nN];
ON: [oO][nN];
INDEX: [iI][nN][dD][eE][xX];
DATABASE: [dD][aA][tT][aA][bB][aA][sS][eE];
UNSIGNED: [uU][nN][sS][iI][gG][nN][eE][dD];
DEFAULT: [dD][eE][fF][aA][uU][lL][tT];
CHECK: [cC][hH][eE][cC][kK];
REFER: REFERENCES;
AND: [aA][nN][dD];
OR: [oO][rR];
NULL: [nN][uU][lL][lL];
INSERT: [iI][nN][sS][eE][rR][tT];
VALUES: [vV][aA][lL][uU][eE][sS];
INTO: [iI][nN][tT][oO];
TRUNCATE: [tT][rR][uU][nN][cC][aA][tT][eE];

LPAREN: '(';
RPAREN: ')';
COMMA: ',';
SEMI: ';';
DOT: '.';
STAR: '*';

EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

STRING_LITERAL
    : '\'' ( ~('\'') | '\'\'' )* '\''
    ;

NUMERIC_LITERAL
    : [0-9]+ ('.' [0-9]+)?
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | [0-9] | '_' | '$')*
    | '"' (~'"')+ '"'
    | '`' (~'`')+ '`'
    ;

fragment LETTER
    : [a-zA-Z]
    ;

WS
    : [ \t\r\n]+ -> skip
    ;

LINE_COMMENT
    : '--' ~[\r\n]* -> skip
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;
