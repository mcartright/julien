grammar Julien;
options { language=Java; }
stmt : (assignment|query)+ ;
assignment : ID '=' functionArg ;
query :  function ;
function : ID LP functionArg (',' functionArg)* RP ;
functionArg : function | INT | FLOAT | ID | STRING ;
ID : [a-zA-Z_] [a-zA-Z0-9_]* ;
WS : [ \t\r\n]+ -> skip ;
LP : '(' ;
RP : ')' ;
STRING : '"' ~[\"]* '"' ;
INT : DIGIT+ ;
FLOAT : DIGIT+ '.' DIGIT+ ;
fragment DIGIT : [0-9] ;
LINE_COMMENT : '//' ~[\r\n]* '\r'? '\n' -> skip ;
COMMENT : '/*' .*? '*/' -> skip ;
