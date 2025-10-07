#pragma once
#include <string>
#include <vector>
#include <memory>

enum class ExpressionType {
    COLUMN,
    LITERAL,
    BINARY_OP,
    FUNCTION_CALL
};

enum class BinaryOperator {
    EQUALS,
    NOT_EQUALS,
    GREATER,
    LESS,
    GREATER_EQUAL,
    LESS_EQUAL,
    AND,
    OR
};

struct Expression {
    ExpressionType type;
    
    Expression(ExpressionType t) : type(t) {}
    virtual ~Expression() = default;
};

struct ColumnExpression : Expression {
    std::string table_name;
    std::string column_name;
    
    ColumnExpression(const std::string& table, const std::string& column)
        : Expression(ExpressionType::COLUMN), table_name(table), column_name(column) {}
};

struct LiteralExpression : Expression {
    std::string value;
    
    LiteralExpression(const std::string& val)
        : Expression(ExpressionType::LITERAL), value(val) {}
};

struct BinaryOpExpression : Expression {
    std::unique_ptr<Expression> left;
    std::unique_ptr<Expression> right;
    BinaryOperator op;
    
    BinaryOpExpression(std::unique_ptr<Expression> l, std::unique_ptr<Expression> r, BinaryOperator operation)
        : Expression(ExpressionType::BINARY_OP), left(std::move(l)), right(std::move(r)), op(operation) {}
};

struct SelectItem {
    std::unique_ptr<Expression> expression;
    std::string alias;
    
    SelectItem(std::unique_ptr<Expression> expr, const std::string& alias_name = "")
        : expression(std::move(expr)), alias(alias_name) {}
};

struct TableReference {
    std::string table_name;
    std::string alias;
    
    TableReference() = default;
    TableReference(const std::string& name, const std::string& table_alias = "")
        : table_name(name), alias(table_alias) {}
};

struct JoinClause {
    enum Type { INNER, LEFT, RIGHT };
    
    Type join_type;
    TableReference table;
    std::unique_ptr<Expression> condition;
    
    JoinClause(Type type, const TableReference& tbl, std::unique_ptr<Expression> cond)
        : join_type(type), table(tbl), condition(std::move(cond)) {}
};

struct SelectStatement {
    std::vector<SelectItem> select_list;
    TableReference from_table;
    std::vector<JoinClause> joins;
    std::unique_ptr<Expression> where_clause;
    
    SelectStatement() = default;
};