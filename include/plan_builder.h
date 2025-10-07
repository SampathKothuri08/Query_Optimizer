#pragma once
#include "query_plan.h"
#include "ast.h"
#include <memory>

class PlanBuilder {
private:
    std::unordered_map<std::string, Statistics> table_stats;
    
    std::string expression_to_string(const Expression& expr);
    JoinType convert_join_type(JoinClause::Type type);
    
public:
    std::unique_ptr<PlanNode> build_scan_node(const TableReference& table);
    std::unique_ptr<PlanNode> build_filter_node(std::unique_ptr<PlanNode> child, const Expression& condition);
    std::unique_ptr<PlanNode> build_project_node(std::unique_ptr<PlanNode> child, const std::vector<SelectItem>& items);
    std::unique_ptr<PlanNode> build_join_node(std::unique_ptr<PlanNode> left, std::unique_ptr<PlanNode> right, 
                                            const JoinClause& join, PlanNodeType join_algorithm);
    PlanBuilder();
    
    void set_table_statistics(const std::string& table_name, const Statistics& stats);
    std::unique_ptr<PlanNode> build_plan(const SelectStatement& stmt);
    
    std::vector<std::unique_ptr<PlanNode>> generate_join_orders(const SelectStatement& stmt);
    std::unique_ptr<PlanNode> optimize_plan(std::unique_ptr<PlanNode> plan);
};