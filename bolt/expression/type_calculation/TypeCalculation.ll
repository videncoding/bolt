%{
#include "bolt/expression/type_calculation/TypeCalculation.yy.h"  // @manual
#include "bolt/expression/type_calculation/Scanner.h"
#define YY_DECL int bytedance::bolt::expression::calculate::Scanner::lex(bytedance::bolt::expression::calculate::Parser::semantic_type *yylval)
%}

%option c++ noyywrap noyylineno nodefault

integer         ([[:digit:]]+)
var             ([[:alpha:]][[:alnum:]_]*)

%%

"+"             return Parser::token::PLUS;
"-"             return Parser::token::MINUS;
"*"             return Parser::token::MULTIPLY;
"/"             return Parser::token::DIVIDE;
"%"             return Parser::token::MODULO;
"("             return Parser::token::LPAREN;
")"             return Parser::token::RPAREN;
","             return Parser::token::COMMA;
"="             return Parser::token::ASSIGN;
"min"           return Parser::token::MIN;
"max"           return Parser::token::MAX;
"<"             return Parser::token::LT;
"<="            return Parser::token::LTE;
">"             return Parser::token::GT;
">="            return Parser::token::GTE;
"=="            return Parser::token::EQ;
"!="            return Parser::token::NEQ;
"?"             return Parser::token::TERNARY;
":"             return Parser::token::COLON;
{integer}       yylval->build<long long>(strtoll(YYText(), nullptr, 10)); return Parser::token::INT;
{var}           yylval->build<std::string>(YYText()); return Parser::token::VAR;
<<EOF>>         return Parser::token::YYEOF;
.               /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "bolt/expression/type_calculation/TypeCalculation.h"

void bytedance::bolt::expression::calculation::evaluate(const std::string& calculation, std::unordered_map<std::string, int>& variables) {
    std::istringstream is(calculation);
    bytedance::bolt::expression::calculate::Scanner scanner{ is, std::cerr, variables};
    bytedance::bolt::expression::calculate::Parser parser{ &scanner };
    parser.parse();
}
