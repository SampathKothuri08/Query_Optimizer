#include <iostream>
#include "tokenizer.h"
#include "parser.h"
#include "optimizer.h"
#include "executor.h"
#include "benchmark.h"

void demonstrate_component(const std::string& title, std::function<void()> demo) {
    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << title << std::endl;
    std::cout << std::string(50, '=') << std::endl;
    demo();
    std::cout << std::endl;
}

int main() {
    std::cout << "🚀 COMPLETE QUERY OPTIMIZER DEMONSTRATION 🚀" << std::endl;
    
    TableManager tm;
    tm.populate_sample_data();
    
    demonstrate_component("1. SQL TOKENIZER", []() {
        std::string sql = "SELECT name, age FROM users WHERE age > 25";
        std::cout << "SQL: " << sql << std::endl;
        
        Tokenizer tokenizer(sql);
        auto tokens = tokenizer.tokenize();
        
        std::cout << "Tokens: ";
        for (const auto& token : tokens) {
            std::cout << token.value << " ";
        }
        std::cout << "\nTotal tokens: " << tokens.size() << std::endl;
    });
    
    demonstrate_component("2. QUERY PLAN GENERATION", []() {
        auto scan = std::make_unique<TableScanNode>("users");
        auto filter = std::make_unique<FilterNode>("age > 25");
        filter->children.push_back(std::move(scan));
        
        std::vector<std::string> projections = {"name", "age"};
        auto project = std::make_unique<ProjectNode>(projections);
        project->children.push_back(std::move(filter));
        
        std::cout << "Generated Plan:\n" << project->to_string() << std::endl;
    });
    
    demonstrate_component("3. COST-BASED OPTIMIZATION", [&tm]() {
        QueryOptimizer optimizer;
        Executor executor(&tm);
        
        SelectStatement stmt;
        stmt.from_table = TableReference("users");
        
        JoinClause join(JoinClause::INNER, TableReference("orders"), 
                       std::make_unique<BinaryOpExpression>(
                           std::make_unique<ColumnExpression>("users", "id"),
                           std::make_unique<ColumnExpression>("orders", "user_id"),
                           BinaryOperator::EQUALS
                       ));
        stmt.joins.push_back(std::move(join));
        
        SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
        stmt.select_list.push_back(std::move(all_item));
        
        auto candidates = optimizer.generate_all_plans(stmt);
        
        std::cout << "Plan alternatives generated: " << candidates.size() << std::endl;
        
        auto best_plan = optimizer.select_best_plan(candidates);
        std::cout << "Best plan selected with cost: " << best_plan->cost.total_cost << std::endl;
        
        auto result = executor.execute(*best_plan);
        std::cout << "Query executed successfully: " << result->size() << " rows returned" << std::endl;
    });
    
    demonstrate_component("4. JOIN ALGORITHM COMPARISON", [&tm]() {
        QueryBenchmark benchmark(&tm);
        
        SelectStatement join_stmt;
        join_stmt.from_table = TableReference("users");
        
        JoinClause join(JoinClause::INNER, TableReference("orders"), 
                       std::make_unique<BinaryOpExpression>(
                           std::make_unique<ColumnExpression>("users", "id"),
                           std::make_unique<ColumnExpression>("orders", "user_id"),
                           BinaryOperator::EQUALS
                       ));
        join_stmt.joins.push_back(std::move(join));
        
        SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
        join_stmt.select_list.push_back(std::move(all_item));
        
        benchmark.compare_join_algorithms(join_stmt);
    });
    
    demonstrate_component("5. PERFORMANCE STATISTICS", [&tm]() {
        std::cout << "Database Statistics:" << std::endl;
        std::cout << "- Users table: " << tm.get_table("users")->row_count() << " rows" << std::endl;
        std::cout << "- Orders table: " << tm.get_table("orders")->row_count() << " rows" << std::endl;
        
        std::cout << "\nOptimizer Features:" << std::endl;
        std::cout << "✓ SQL parsing and tokenization" << std::endl;
        std::cout << "✓ Abstract syntax tree generation" << std::endl;
        std::cout << "✓ Query plan tree representation" << std::endl;
        std::cout << "✓ Three join algorithms (Nested Loop, Hash, Sort-Merge)" << std::endl;
        std::cout << "✓ Cost-based plan selection" << std::endl;
        std::cout << "✓ Plan enumeration and optimization" << std::endl;
        std::cout << "✓ Real query execution engine" << std::endl;
        std::cout << "✓ Performance benchmarking framework" << std::endl;
    });
    
    std::cout << "\n" << std::string(60, '=') << std::endl;
    std::cout << "=== QUERY OPTIMIZER PROJECT COMPLETE! ===" << std::endl;
    std::cout << std::string(60, '=') << std::endl;
    std::cout << "\nAll components implemented successfully:" << std::endl;
    std::cout << "• Rule-based and cost-based query optimization ✓" << std::endl;
    std::cout << "• SQL parser with AST generation ✓" << std::endl;
    std::cout << "• Query plan tree representation ✓" << std::endl;
    std::cout << "• Multiple join algorithm implementations ✓" << std::endl;
    std::cout << "• I/O cost models and selectivity estimation ✓" << std::endl;
    std::cout << "• Plan enumeration with cost-based selection ✓" << std::endl;
    std::cout << "• Performance evaluation and benchmarking ✓" << std::endl;
    
    return 0;
}