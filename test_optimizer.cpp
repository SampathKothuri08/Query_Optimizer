#include <iostream>
#include "table.h"
#include "optimizer.h"
#include "executor.h"

int main() {
    std::cout << "Query Optimizer Test" << std::endl;
    
    TableManager tm;
    tm.populate_sample_data();
    
    QueryOptimizer optimizer;
    Executor executor(&tm);
    
    std::cout << "\n=== Test 1: Single Table Query ===" << std::endl;
    
    SelectStatement simple_stmt;
    simple_stmt.from_table = TableReference("users");
    
    SelectItem name_item(std::make_unique<ColumnExpression>("", "name"));
    SelectItem age_item(std::make_unique<ColumnExpression>("", "age"));
    simple_stmt.select_list.push_back(std::move(name_item));
    simple_stmt.select_list.push_back(std::move(age_item));
    
    simple_stmt.where_clause = std::make_unique<BinaryOpExpression>(
        std::make_unique<ColumnExpression>("", "age"),
        std::make_unique<LiteralExpression>("25"),
        BinaryOperator::GREATER
    );
    
    auto optimized_plan = optimizer.optimize(simple_stmt);
    
    std::cout << "Optimized Plan:" << std::endl;
    std::cout << optimized_plan->to_string() << std::endl;
    std::cout << "Estimated Cost: " << optimized_plan->cost.total_cost << std::endl;
    
    std::cout << "\n=== Test 2: Join Query with Different Algorithms ===" << std::endl;
    
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
    
    auto candidates = optimizer.generate_all_plans(join_stmt);
    optimizer.print_optimization_report(candidates);
    
    auto best_plan = optimizer.select_best_plan(candidates);
    
    std::cout << "\nExecuting best plan (first 3 rows):" << std::endl;
    auto result = executor.execute(*best_plan);
    result->print(3);
    
    std::cout << "\n=== Performance Comparison ===" << std::endl;
    std::cout << "Manual plans vs. Optimizer:" << std::endl;
    
    auto manual_nested = std::make_unique<NestedLoopJoinNode>(JoinType::INNER, "users.id = orders.user_id");
    auto users_scan = std::make_unique<TableScanNode>("users");
    auto orders_scan = std::make_unique<TableScanNode>("orders");
    manual_nested->children.push_back(std::move(users_scan));
    manual_nested->children.push_back(std::move(orders_scan));
    
    CostModel cost_model;
    auto manual_cost = cost_model.estimate_plan_cost(*manual_nested);
    
    std::cout << "Manual Nested Loop Cost: " << manual_cost.total_cost << std::endl;
    std::cout << "Optimizer Best Cost: " << best_plan->cost.total_cost << std::endl;
    
    double improvement = ((manual_cost.total_cost - best_plan->cost.total_cost) / manual_cost.total_cost) * 100.0;
    std::cout << "Improvement: " << improvement << "%" << std::endl;
    
    return 0;
}