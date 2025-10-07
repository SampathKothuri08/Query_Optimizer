#include <iostream>
#include "table.h"
#include "query_plan.h"
#include "executor.h"

int main() {
    std::cout << "Query Executor Test" << std::endl;
    
    TableManager tm;
    tm.populate_sample_data();
    
    Executor executor(&tm);
    
    auto scan = std::make_unique<TableScanNode>("users");
    scan->stats = Statistics(1000, 10, 1.0);
    
    auto filter = std::make_unique<FilterNode>("age > 25");
    filter->stats = Statistics(100, 1, 0.1);
    filter->children.push_back(std::move(scan));
    
    std::vector<std::string> projections = {"name", "age"};
    auto project = std::make_unique<ProjectNode>(projections);
    project->stats = Statistics(100, 1, 1.0);
    project->children.push_back(std::move(filter));
    
    std::cout << "\nExecuting plan:" << std::endl;
    std::cout << project->to_string() << std::endl;
    
    auto result = executor.execute(*project);
    
    std::cout << "\nQuery Result:" << std::endl;
    result->print(5);
    
    std::cout << "\nTesting Join Operations:" << std::endl;
    
    auto users_scan = std::make_unique<TableScanNode>("users");
    auto orders_scan = std::make_unique<TableScanNode>("orders");
    
    auto nested_join = std::make_unique<NestedLoopJoinNode>(JoinType::INNER, "users.id = orders.user_id");
    nested_join->children.push_back(std::move(users_scan));
    nested_join->children.push_back(std::move(orders_scan));
    
    std::cout << "\nNested Loop Join (first 3 rows):" << std::endl;
    auto join_result = executor.execute(*nested_join);
    join_result->print(3);
    
    return 0;
}