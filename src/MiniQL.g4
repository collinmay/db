grammar MiniQL;
statement: 'SELECT ' ('*' | columnList) ' FROM ' tableName ';';
columnList: columnName (',' columnList)?;
columnName: IDENTIFIER;
tableName: IDENTIFIER;

IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;

WS: [ \n] -> skip;
