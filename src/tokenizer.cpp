#include "tokenizer.h"
#include <cctype>
#include <algorithm>

Tokenizer::Tokenizer(const std::string& sql_query) : sql(sql_query), current(0) {
    initKeywords();
}

void Tokenizer::initKeywords() {
    keywords["SELECT"] = TokenType::SELECT;
    keywords["FROM"] = TokenType::FROM;
    keywords["WHERE"] = TokenType::WHERE;
    keywords["JOIN"] = TokenType::JOIN;
    keywords["INNER"] = TokenType::INNER;
    keywords["LEFT"] = TokenType::LEFT;
    keywords["RIGHT"] = TokenType::RIGHT;
    keywords["ON"] = TokenType::ON;
    keywords["AND"] = TokenType::AND;
    keywords["OR"] = TokenType::OR;
}

char Tokenizer::peek() const {
    if (isAtEnd()) return '\0';
    return sql[current];
}

char Tokenizer::advance() {
    if (isAtEnd()) return '\0';
    return sql[current++];
}

void Tokenizer::skipWhitespace() {
    while (!isAtEnd() && std::isspace(peek())) {
        advance();
    }
}

bool Tokenizer::isAtEnd() const {
    return current >= sql.length();
}

Token Tokenizer::parseString() {
    size_t start = current;
    advance(); // Skip opening quote
    
    std::string value;
    while (!isAtEnd() && peek() != '\'') {
        value += advance();
    }
    
    if (!isAtEnd()) {
        advance(); // Skip closing quote
    }
    
    return Token(TokenType::STRING, value, start);
}

Token Tokenizer::parseNumber() {
    size_t start = current;
    std::string value;
    
    while (!isAtEnd() && (std::isdigit(peek()) || peek() == '.')) {
        value += advance();
    }
    
    return Token(TokenType::NUMBER, value, start);
}

Token Tokenizer::parseIdentifier() {
    size_t start = current;
    std::string value;
    
    while (!isAtEnd() && (std::isalnum(peek()) || peek() == '_')) {
        value += advance();
    }
    
    // Convert to uppercase for keyword lookup
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    
    auto it = keywords.find(upper_value);
    TokenType type = (it != keywords.end()) ? it->second : TokenType::IDENTIFIER;
    
    return Token(type, value, start);
}

Token Tokenizer::parseOperator() {
    size_t start = current;
    char c = advance();
    
    switch (c) {
        case '=': return Token(TokenType::EQUALS, "=", start);
        case '>':
            if (!isAtEnd() && peek() == '=') {
                advance();
                return Token(TokenType::GREATER_EQUAL, ">=", start);
            }
            return Token(TokenType::GREATER, ">", start);
        case '<':
            if (!isAtEnd() && peek() == '=') {
                advance();
                return Token(TokenType::LESS_EQUAL, "<=", start);
            } else if (!isAtEnd() && peek() == '>') {
                advance();
                return Token(TokenType::NOT_EQUAL, "<>", start);
            }
            return Token(TokenType::LESS, "<", start);
        case ',': return Token(TokenType::COMMA, ",", start);
        case ';': return Token(TokenType::SEMICOLON, ";", start);
        case '(': return Token(TokenType::LEFT_PAREN, "(", start);
        case ')': return Token(TokenType::RIGHT_PAREN, ")", start);
        case '.': return Token(TokenType::DOT, ".", start);
        case '*': return Token(TokenType::ASTERISK, "*", start);
        default: return Token(TokenType::UNKNOWN, std::string(1, c), start);
    }
}

Token Tokenizer::nextToken() {
    skipWhitespace();
    
    if (isAtEnd()) {
        return Token(TokenType::END_OF_FILE, "", current);
    }
    
    char c = peek();
    
    if (c == '\'') {
        return parseString();
    }
    
    if (std::isdigit(c)) {
        return parseNumber();
    }
    
    if (std::isalpha(c) || c == '_') {
        return parseIdentifier();
    }
    
    return parseOperator();
}

std::vector<Token> Tokenizer::tokenize() {
    std::vector<Token> tokens;
    
    while (!isAtEnd()) {
        Token token = nextToken();
        tokens.push_back(token);
        
        if (token.type == TokenType::END_OF_FILE) {
            break;
        }
    }
    
    return tokens;
}