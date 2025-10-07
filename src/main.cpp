#include <iostream>
#include "tokenizer.h"
#include "parser.h"

void printExpression(const Expression& expr, int indent = 0) {
    std::string spaces(indent * 2, ' ');
    
    switch (expr.type) {
        case ExpressionType::COLUMN: {
            const auto& col = static_cast<const ColumnExpression&>(expr);
            std::cout << spaces << "Column: " << col.table_name << "." << col.column_name << std::endl;
            break;
        }
        case ExpressionType::LITERAL: {
            const auto& lit = static_cast<const LiteralExpression&>(expr);
            std::cout << spaces << "Literal: " << lit.value << std::endl;
            break;
        }
        case ExpressionType::BINARY_OP: {
            const auto& binop = static_cast<const BinaryOpExpression&>(expr);
            std::cout << spaces << "BinaryOp: " << static_cast<int>(binop.op) << std::endl;
            printExpression(*binop.left, indent + 1);
            printExpression(*binop.right, indent + 1);
            break;
        }
        default:
            std::cout << spaces << "Unknown expression type" << std::endl;
    }
}

int main() {
    std::cout << "Query Optimizer - Parser Test" << std::endl;
    
    try {
        std::string sql = "SELECT name, age FROM users WHERE age > 25";
        std::cout << "Parsing: " << sql << std::endl;
        
        Tokenizer tokenizer(sql);
        auto tokens = tokenizer.tokenize();
        std::cout << "Tokenized successfully, " << tokens.size() << " tokens" << std::endl;
        
        for (const auto& token : tokens) {
            std::cout << "Token: " << token.value << " (Type: " << static_cast<int>(token.type) << ")" << std::endl;
        }
        
        Parser parser(tokens);
        std::cout << "Starting to parse..." << std::endl;
        
        // Test basic parser functions
        std::cout << "Parser current position: " << parser.current << std::endl;
        
        return 0;
        
        auto ast = parser.parseSelectStatement();
        std::cout << "Parsed successfully!" << std::endl;
    
        std::cout << "\nParsed SELECT statement:" << std::endl;
        std::cout << "SELECT items: " << ast->select_list.size() << std::endl;
        
        for (size_t i = 0; i < ast->select_list.size(); ++i) {
            std::cout << "Item " << i << ":" << std::endl;
            printExpression(*ast->select_list[i].expression, 1);
        }
        
        std::cout << "\nFROM table: " << ast->from_table.table_name << std::endl;
        
        if (ast->where_clause) {
            std::cout << "\nWHERE clause:" << std::endl;
            printExpression(*ast->where_clause, 1);
        }
        
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
    }
    
    return 0;
}