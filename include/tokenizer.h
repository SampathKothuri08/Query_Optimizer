#pragma once
#include <string>
#include <vector>
#include <unordered_map>

enum class TokenType {
    SELECT,
    FROM, 
    WHERE,
    JOIN,
    INNER,
    LEFT,
    RIGHT,
    ON,
    AND,
    OR,
    EQUALS,
    GREATER,
    LESS,
    GREATER_EQUAL,
    LESS_EQUAL,
    NOT_EQUAL,
    IDENTIFIER,
    NUMBER,
    STRING,
    COMMA,
    SEMICOLON,
    LEFT_PAREN,
    RIGHT_PAREN,
    DOT,
    ASTERISK,
    END_OF_FILE,
    UNKNOWN
};

struct Token {
    TokenType type;
    std::string value;
    size_t position;
    
    Token(TokenType t, const std::string& v, size_t pos) 
        : type(t), value(v), position(pos) {}
};

class Tokenizer {
private:
    std::string sql;
    size_t current;
    std::unordered_map<std::string, TokenType> keywords;
    
    void initKeywords();
    char peek() const;
    char advance();
    void skipWhitespace();
    Token parseString();
    Token parseNumber();
    Token parseIdentifier();
    Token parseOperator();
    
public:
    explicit Tokenizer(const std::string& sql_query);
    std::vector<Token> tokenize();
    Token nextToken();
    bool isAtEnd() const;
};