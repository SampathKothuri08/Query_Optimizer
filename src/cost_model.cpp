#include "cost_model.h"
#include <algorithm>
#include <iostream>

CostModel::CostModel() {
    TableStatistics users_stats(1000, 10, 120);
    users_stats.column_selectivity["age > 25"] = 0.88;
    users_stats.column_selectivity["age < 30"] = 0.20;
    users_stats.distinct_values["id"] = 1000;
    users_stats.distinct_values["age"] = 50;
    users_stats.distinct_values["city"] = 10;
    table_stats["users"] = users_stats;
    
    TableStatistics orders_stats(5000, 50, 80);
    orders_stats.column_selectivity["amount > 100"] = 0.30;
    orders_stats.distinct_values["id"] = 5000;
    orders_stats.distinct_values["user_id"] = 1000;
    orders_stats.distinct_values["product"] = 100;
    table_stats["orders"] = orders_stats;
}

void CostModel::set_table_statistics(const std::string& table_name, const TableStatistics& stats) {
    table_stats[table_name] = stats;
}

double CostModel::estimate_scan_cost(const TableStatistics& stats) {
    return stats.page_count * CostConstants::SEQUENTIAL_IO_COST + 
           stats.tuple_count * CostConstants::CPU_TUPLE_COST;
}

double CostModel::estimate_filter_cost(size_t input_tuples, double selectivity) {
    return input_tuples * CostConstants::CPU_OPERATOR_COST;
}

double CostModel::estimate_nested_loop_cost(size_t left_tuples, size_t right_tuples, size_t right_pages) {
    double io_cost = left_tuples * right_pages * CostConstants::RANDOM_IO_COST;
    double cpu_cost = left_tuples * right_tuples * CostConstants::CPU_OPERATOR_COST;
    return io_cost + cpu_cost;
}

double CostModel::estimate_hash_join_cost(size_t build_tuples, size_t probe_tuples, size_t build_pages) {
    double build_cost = build_tuples * CostConstants::HASH_BUILD_COST;
    double probe_cost = probe_tuples * CostConstants::HASH_PROBE_COST;
    double io_cost = build_pages * CostConstants::SEQUENTIAL_IO_COST;
    return build_cost + probe_cost + io_cost;
}

double CostModel::estimate_sort_cost(size_t tuple_count) {
    if (tuple_count <= 1) return 0.0;
    return tuple_count * log2_safe(tuple_count) * CostConstants::CPU_OPERATOR_COST * CostConstants::MEMORY_SORT_COST;
}

double CostModel::estimate_sort_merge_cost(size_t left_tuples, size_t right_tuples) {
    double left_sort = estimate_sort_cost(left_tuples);
    double right_sort = estimate_sort_cost(right_tuples);
    double merge_cost = (left_tuples + right_tuples) * CostConstants::CPU_OPERATOR_COST;
    return left_sort + right_sort + merge_cost;
}

CostEstimate CostModel::estimate_table_scan_cost(const TableScanNode& node) {
    auto it = table_stats.find(node.table_name);
    if (it == table_stats.end()) {
        return CostEstimate(10.0, 1.0);
    }
    
    const auto& stats = it->second;
    double total_cost = estimate_scan_cost(stats);
    
    return CostEstimate(stats.page_count * CostConstants::SEQUENTIAL_IO_COST, 
                       stats.tuple_count * CostConstants::CPU_TUPLE_COST);
}

CostEstimate CostModel::estimate_filter_cost(const FilterNode& node) {
    if (node.children.empty()) {
        return CostEstimate();
    }
    
    auto child_cost = estimate_plan_cost(*node.children[0]);
    size_t input_tuples = estimate_output_cardinality(*node.children[0]);
    
    double filter_cpu_cost = estimate_filter_cost(input_tuples, 0.1);
    
    return CostEstimate(child_cost.io_cost, child_cost.cpu_cost + filter_cpu_cost);
}

CostEstimate CostModel::estimate_project_cost(const ProjectNode& node) {
    if (node.children.empty()) {
        return CostEstimate();
    }
    
    auto child_cost = estimate_plan_cost(*node.children[0]);
    size_t input_tuples = estimate_output_cardinality(*node.children[0]);
    
    double project_cpu_cost = input_tuples * CostConstants::CPU_OPERATOR_COST * 0.5;
    
    return CostEstimate(child_cost.io_cost, child_cost.cpu_cost + project_cpu_cost);
}

double CostModel::estimate_join_selectivity(const std::string& condition) {
    if (condition.find("=") != std::string::npos) {
        return 0.1;
    } else if (condition.find(">") != std::string::npos || condition.find("<") != std::string::npos) {
        return 0.33;
    }
    return 0.1;
}

CostEstimate CostModel::estimate_join_cost(const JoinNode& node) {
    if (node.children.size() < 2) {
        return CostEstimate();
    }
    
    auto left_cost = estimate_plan_cost(*node.children[0]);
    auto right_cost = estimate_plan_cost(*node.children[1]);
    
    size_t left_tuples = estimate_output_cardinality(*node.children[0]);
    size_t right_tuples = estimate_output_cardinality(*node.children[1]);
    
    double total_io = left_cost.io_cost + right_cost.io_cost;
    double total_cpu = left_cost.cpu_cost + right_cost.cpu_cost;
    
    switch (node.type) {
        case PlanNodeType::NESTED_LOOP_JOIN: {
            size_t right_pages = std::max(1UL, right_tuples / 100);
            double join_cost = estimate_nested_loop_cost(left_tuples, right_tuples, right_pages);
            return CostEstimate(total_io + left_tuples * right_pages * CostConstants::RANDOM_IO_COST, 
                              total_cpu + join_cost);
        }
        
        case PlanNodeType::HASH_JOIN: {
            size_t build_tuples = std::min(left_tuples, right_tuples);
            size_t probe_tuples = std::max(left_tuples, right_tuples);
            size_t build_pages = std::max(1UL, build_tuples / 100);
            double join_cost = estimate_hash_join_cost(build_tuples, probe_tuples, build_pages);
            return CostEstimate(total_io, total_cpu + join_cost);
        }
        
        case PlanNodeType::SORT_MERGE_JOIN: {
            double join_cost = estimate_sort_merge_cost(left_tuples, right_tuples);
            return CostEstimate(total_io, total_cpu + join_cost);
        }
        
        default:
            return CostEstimate(total_io, total_cpu);
    }
}

size_t CostModel::estimate_output_cardinality(const PlanNode& node) {
    switch (node.type) {
        case PlanNodeType::TABLE_SCAN: {
            const auto& scan_node = static_cast<const TableScanNode&>(node);
            auto it = table_stats.find(scan_node.table_name);
            return (it != table_stats.end()) ? it->second.tuple_count : 1000;
        }
        
        case PlanNodeType::FILTER: {
            if (node.children.empty()) return 0;
            const auto& filter_node = static_cast<const FilterNode&>(node);
            size_t input_cardinality = estimate_output_cardinality(*node.children[0]);
            
            double selectivity = 0.1;
            if (filter_node.condition.find("age > 25") != std::string::npos) {
                selectivity = 0.88;
            } else if (filter_node.condition.find("age < 30") != std::string::npos) {
                selectivity = 0.20;
            }
            
            return static_cast<size_t>(input_cardinality * selectivity);
        }
        
        case PlanNodeType::PROJECT: {
            if (node.children.empty()) return 0;
            return estimate_output_cardinality(*node.children[0]);
        }
        
        case PlanNodeType::NESTED_LOOP_JOIN:
        case PlanNodeType::HASH_JOIN:
        case PlanNodeType::SORT_MERGE_JOIN: {
            if (node.children.size() < 2) return 0;
            const auto& join_node = static_cast<const JoinNode&>(node);
            
            size_t left_cardinality = estimate_output_cardinality(*node.children[0]);
            size_t right_cardinality = estimate_output_cardinality(*node.children[1]);
            
            double selectivity = estimate_join_selectivity(join_node.join_condition);
            return static_cast<size_t>(left_cardinality * right_cardinality * selectivity);
        }
        
        default:
            return 1000;
    }
}

CostEstimate CostModel::estimate_plan_cost(const PlanNode& node) {
    switch (node.type) {
        case PlanNodeType::TABLE_SCAN:
            return estimate_table_scan_cost(static_cast<const TableScanNode&>(node));
        case PlanNodeType::FILTER:
            return estimate_filter_cost(static_cast<const FilterNode&>(node));
        case PlanNodeType::PROJECT:
            return estimate_project_cost(static_cast<const ProjectNode&>(node));
        case PlanNodeType::NESTED_LOOP_JOIN:
        case PlanNodeType::HASH_JOIN:
        case PlanNodeType::SORT_MERGE_JOIN:
            return estimate_join_cost(static_cast<const JoinNode&>(node));
        default:
            return CostEstimate();
    }
}