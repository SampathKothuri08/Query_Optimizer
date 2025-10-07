#pragma once
#include "query_plan.h"
#include <unordered_map>
#include <cmath>

struct CostConstants {
    static constexpr double SEQUENTIAL_IO_COST = 1.0;
    static constexpr double RANDOM_IO_COST = 4.0;
    static constexpr double CPU_TUPLE_COST = 0.01;
    static constexpr double CPU_OPERATOR_COST = 0.0025;
    static constexpr double MEMORY_SORT_COST = 2.0;
    static constexpr double HASH_BUILD_COST = 1.0;
    static constexpr double HASH_PROBE_COST = 0.5;
};

struct TableStatistics {
    size_t tuple_count;
    size_t page_count;
    size_t tuple_width;
    std::unordered_map<std::string, double> column_selectivity;
    std::unordered_map<std::string, size_t> distinct_values;
    
    TableStatistics(size_t tuples = 0, size_t pages = 0, size_t width = 100)
        : tuple_count(tuples), page_count(pages), tuple_width(width) {}
        
    double get_selectivity(const std::string& condition) const {
        auto it = column_selectivity.find(condition);
        return (it != column_selectivity.end()) ? it->second : 0.1;
    }
};

class CostModel {
private:
    std::unordered_map<std::string, TableStatistics> table_stats;
    
    double estimate_scan_cost(const TableStatistics& stats);
    double estimate_filter_cost(size_t input_tuples, double selectivity);
    double estimate_nested_loop_cost(size_t left_tuples, size_t right_tuples, size_t right_pages);
    double estimate_hash_join_cost(size_t build_tuples, size_t probe_tuples, size_t build_pages);
    double estimate_sort_merge_cost(size_t left_tuples, size_t right_tuples);
    double estimate_sort_cost(size_t tuple_count);
    
    double log2_safe(double x) {
        return (x <= 1.0) ? 0.0 : std::log2(x);
    }
    
public:
    CostModel();
    
    void set_table_statistics(const std::string& table_name, const TableStatistics& stats);
    CostEstimate estimate_plan_cost(const PlanNode& node);
    
    CostEstimate estimate_table_scan_cost(const TableScanNode& node);
    CostEstimate estimate_filter_cost(const FilterNode& node);
    CostEstimate estimate_project_cost(const ProjectNode& node);
    CostEstimate estimate_join_cost(const JoinNode& node);
    
    double estimate_join_selectivity(const std::string& condition);
    size_t estimate_output_cardinality(const PlanNode& node);
};