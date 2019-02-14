grammar MiniQL;
statement: 'SELECT ' ('*' | columnList) ' FROM ' tableName (' WHERE ' whereFilter = expression)? ';' EOF;
columnList: columnName (',' columnList)?;
columnName: IDENTIFIER;
tableName: IDENTIFIER;

expression
  : LPAREN expression RPAREN # parenExpression
  | left=expression op=comparator right=expression # comparisonExpression
  | columnName # columnExpression
  | literal # literalExpression
  ;
comparator: (LT | GT | EQ | NE);

literal: INTEGER_LITERAL | STRING_LITERAL;

LT: '<';
GT: '>';
EQ: '=';
NE: '!=';
LPAREN: '(';
RPAREN: ')';
IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;
INTEGER_LITERAL: '-'?[0-9]+;
STRING_LITERAL: '"' .*? '"';

WS: [ \n] -> skip;
