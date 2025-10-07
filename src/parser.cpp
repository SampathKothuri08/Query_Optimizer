#include "parser.h"
#include <stdexcept>
#include <iostream>

Parser::Parser(const std::vector<Token>& token_list) : tokens(token_list), current(0) {}

Token Parser::peek() const {
    if (isAtEnd()) return Token(TokenType::END_OF_FILE, "", tokens.size());
    return tokens[current];
}

Token Parser::previous() const {
    return tokens[current - 1];
}

Token Parser::advance() {
    if (!isAtEnd()) current++;
    return previous();
}

bool Parser::isAtEnd() const {
    return current >= tokens.size() || peek().type == TokenType::END_OF_FILE;
}

bool Parser::check(TokenType type) const {
    if (isAtEnd()) return false;
    return peek().type == type;
}

bool Parser::match(std::initializer_list<TokenType> types) {
    for (TokenType type : types) {
        if (check(type)) {
            advance();
            return true;
        }
    }
    return false;
}

Token Parser::consume(TokenType type, const std::string& message) {
    if (check(type)) return advance();
    throw std::runtime_error("Parse error: " + message + " at token: " + peek().value);
}

BinaryOperator Parser::tokenToBinaryOp(TokenType type) {
    switch (type) {
        case TokenType::EQUALS: return BinaryOperator::EQUALS;
        case TokenType::NOT_EQUAL: return BinaryOperator::NOT_EQUALS;
        case TokenType::GREATER: return BinaryOperator::GREATER;
        case TokenType::LESS: return BinaryOperator::LESS;
        case TokenType::GREATER_EQUAL: return BinaryOperator::GREATER_EQUAL;
        case TokenType::LESS_EQUAL: return BinaryOperator::LESS_EQUAL;
        case TokenType::AND: return BinaryOperator::AND;
        case TokenType::OR: return BinaryOperator::OR;
        default: throw std::runtime_error("Invalid binary operator");
    }
}

std::unique_ptr<Expression> Parser::parseColumnOrLiteral() {
    if (check(TokenType::IDENTIFIER)) {
        std::string first = advance().value;
        
        if (match({TokenType::DOT})) {
            std::string column = consume(TokenType::IDENTIFIER, "Expected column name after '.'").value;
            return std::make_unique<ColumnExpression>(first, column);
        } else {
            return std::make_unique<ColumnExpression>("", first);
        }
    }
    
    if (check(TokenType::NUMBER) || check(TokenType::STRING)) {
        return std::make_unique<LiteralExpression>(advance().value);
    }
    
    throw std::runtime_error("Expected identifier, number, or string");
}

std::unique_ptr<Expression> Parser::parsePrimary() {
    if (match({TokenType::LEFT_PAREN})) {
        auto expr = parseExpression();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after expression");
        return expr;
    }
    
    return parseColumnOrLiteral();
}

std::unique_ptr<Expression> Parser::parseComparison() {
    auto expr = parsePrimary();
    
    while (match({TokenType::GREATER, TokenType::GREATER_EQUAL, TokenType::LESS, 
                  TokenType::LESS_EQUAL, TokenType::EQUALS, TokenType::NOT_EQUAL})) {
        TokenType op = previous().type;
        auto right = parsePrimary();
        expr = std::make_unique<BinaryOpExpression>(std::move(expr), std::move(right), tokenToBinaryOp(op));
    }
    
    return expr;
}

std::unique_ptr<Expression> Parser::parseLogicalAnd() {
    auto expr = parseComparison();
    
    while (match({TokenType::AND})) {
        TokenType op = previous().type;
        auto right = parseComparison();
        expr = std::make_unique<BinaryOpExpression>(std::move(expr), std::move(right), tokenToBinaryOp(op));
    }
    
    return expr;
}

std::unique_ptr<Expression> Parser::parseLogicalOr() {
    auto expr = parseLogicalAnd();
    
    while (match({TokenType::OR})) {
        TokenType op = previous().type;
        auto right = parseLogicalAnd();
        expr = std::make_unique<BinaryOpExpression>(std::move(expr), std::move(right), tokenToBinaryOp(op));
    }
    
    return expr;
}

std::unique_ptr<Expression> Parser::parseExpression() {
    return parseLogicalOr();
}

SelectItem Parser::parseSelectItem() {
    if (match({TokenType::ASTERISK})) {
        return SelectItem(std::make_unique<ColumnExpression>("", "*"));
    }
    
    auto expr = parseExpression();
    return SelectItem(std::move(expr));
}

TableReference Parser::parseTableReference() {
    std::string table_name = consume(TokenType::IDENTIFIER, "Expected table name").value;
    std::string alias = "";
    
    if (check(TokenType::IDENTIFIER)) {
        alias = advance().value;
    }
    
    return TableReference(table_name, alias);
}

JoinClause Parser::parseJoinClause() {
    JoinClause::Type join_type = JoinClause::INNER;
    
    if (match({TokenType::LEFT})) {
        join_type = JoinClause::LEFT;
    } else if (match({TokenType::RIGHT})) {
        join_type = JoinClause::RIGHT;
    } else if (match({TokenType::INNER})) {
        join_type = JoinClause::INNER;
    }
    
    consume(TokenType::JOIN, "Expected JOIN keyword");
    
    auto table = parseTableReference();
    
    consume(TokenType::ON, "Expected ON keyword");
    auto condition = parseExpression();
    
    return JoinClause(join_type, table, std::move(condition));
}

std::unique_ptr<SelectStatement> Parser::parseSelectStatement() {
    auto stmt = std::make_unique<SelectStatement>();
    
    consume(TokenType::SELECT, "Expected SELECT keyword");
    
    stmt->select_list.push_back(parseSelectItem());
    
    while (match({TokenType::COMMA})) {
        stmt->select_list.push_back(parseSelectItem());
    }
    
    consume(TokenType::FROM, "Expected FROM keyword");
    stmt->from_table = parseTableReference();
    
    while (check(TokenType::JOIN) || check(TokenType::INNER) || 
           check(TokenType::LEFT) || check(TokenType::RIGHT)) {
        stmt->joins.push_back(parseJoinClause());
    }
    
    if (match({TokenType::WHERE})) {
        stmt->where_clause = parseExpression();
    }
    
    return stmt;
}