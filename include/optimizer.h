#pragma once
#include "cost_model.h"
#include "plan_builder.h"
#include "ast.h"
#include <vector>

class QueryOptimizer {
private:
    CostModel cost_model;
    PlanBuilder plan_builder;
    
    std::vector<std::unique_ptr<PlanNode>> enumerate_join_orders(const SelectStatement& stmt);
    std::unique_ptr<PlanNode> apply_filter_pushdown(std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> choose_join_algorithm(std::unique_ptr<PlanNode> plan);
    
    std::unique_ptr<PlanNode> optimize_single_table(const SelectStatement& stmt);
    std::unique_ptr<PlanNode> optimize_join_query(const SelectStatement& stmt);
    
    struct PlanCandidate {
        std::unique_ptr<PlanNode> plan;
        CostEstimate cost;
        
        PlanCandidate(std::unique_ptr<PlanNode> p, const CostEstimate& c)
            : plan(std::move(p)), cost(c) {}
    };
    
public:
    QueryOptimizer();
    
    void set_table_statistics(const std::string& table_name, const TableStatistics& stats);
    std::unique_ptr<PlanNode> optimize(const SelectStatement& stmt);
    
    std::vector<PlanCandidate> generate_all_plans(const SelectStatement& stmt);
    std::unique_ptr<PlanNode> select_best_plan(std::vector<PlanCandidate>& candidates);
    
    void print_optimization_report(const std::vector<PlanCandidate>& candidates);
};