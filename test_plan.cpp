#include <iostream>
#include "query_plan.h"
#include "plan_builder.h"

int main() {
    std::cout << "Query Plan Test" << std::endl;
    
    // Create a simple plan manually
    auto scan = std::make_unique<TableScanNode>("users");
    scan->stats = Statistics(1000, 10, 1.0);
    
    auto filter = std::make_unique<FilterNode>("age > 25");
    filter->stats = Statistics(100, 1, 0.1);
    filter->children.push_back(std::move(scan));
    
    std::vector<std::string> projections = {"name", "age"};
    auto project = std::make_unique<ProjectNode>(projections);
    project->stats = Statistics(100, 1, 1.0);
    project->children.push_back(std::move(filter));
    
    std::cout << "\nQuery Plan:" << std::endl;
    std::cout << project->to_string() << std::endl;
    
    auto cost = project->estimate_cost();
    std::cout << "\nCost Estimate:" << std::endl;
    std::cout << "I/O Cost: " << cost.io_cost << std::endl;
    std::cout << "CPU Cost: " << cost.cpu_cost << std::endl;
    std::cout << "Total Cost: " << cost.total_cost << std::endl;
    
    return 0;
}