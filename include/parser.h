#pragma once
#include "tokenizer.h"
#include "ast.h"
#include <memory>

class Parser {
private:
    std::vector<Token> tokens;
    size_t current;
    
    Token peek() const;
    Token previous() const;
    Token advance();
    bool isAtEnd() const;
    bool check(TokenType type) const;
    bool match(std::initializer_list<TokenType> types);
    Token consume(TokenType type, const std::string& message);
    
    std::unique_ptr<Expression> parseExpression();
    std::unique_ptr<Expression> parseLogicalOr();
    std::unique_ptr<Expression> parseLogicalAnd();
    std::unique_ptr<Expression> parseComparison();
    std::unique_ptr<Expression> parsePrimary();
    
    std::unique_ptr<Expression> parseColumnOrLiteral();
    BinaryOperator tokenToBinaryOp(TokenType type);
    
    SelectItem parseSelectItem();
    TableReference parseTableReference();
    JoinClause parseJoinClause();
    
public:
    explicit Parser(const std::vector<Token>& token_list);
    std::unique_ptr<SelectStatement> parseSelectStatement();
};