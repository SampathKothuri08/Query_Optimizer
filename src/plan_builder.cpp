#include "plan_builder.h"
#include <iostream>

PlanBuilder::PlanBuilder() {
    table_stats["users"] = Statistics(1000, 10, 1.0);
    table_stats["orders"] = Statistics(5000, 50, 1.0);
    table_stats["products"] = Statistics(500, 5, 1.0);
}

void PlanBuilder::set_table_statistics(const std::string& table_name, const Statistics& stats) {
    table_stats[table_name] = stats;
}

std::string PlanBuilder::expression_to_string(const Expression& expr) {
    switch (expr.type) {
        case ExpressionType::COLUMN: {
            const auto& col = static_cast<const ColumnExpression&>(expr);
            return col.table_name.empty() ? col.column_name : col.table_name + "." + col.column_name;
        }
        case ExpressionType::LITERAL: {
            const auto& lit = static_cast<const LiteralExpression&>(expr);
            return lit.value;
        }
        case ExpressionType::BINARY_OP: {
            const auto& binop = static_cast<const BinaryOpExpression&>(expr);
            std::string op_str;
            switch (binop.op) {
                case BinaryOperator::EQUALS: op_str = "="; break;
                case BinaryOperator::NOT_EQUALS: op_str = "!="; break;
                case BinaryOperator::GREATER: op_str = ">"; break;
                case BinaryOperator::LESS: op_str = "<"; break;
                case BinaryOperator::GREATER_EQUAL: op_str = ">="; break;
                case BinaryOperator::LESS_EQUAL: op_str = "<="; break;
                case BinaryOperator::AND: op_str = "AND"; break;
                case BinaryOperator::OR: op_str = "OR"; break;
            }
            return "(" + expression_to_string(*binop.left) + " " + op_str + " " + expression_to_string(*binop.right) + ")";
        }
        default:
            return "UNKNOWN_EXPR";
    }
}

JoinType PlanBuilder::convert_join_type(JoinClause::Type type) {
    switch (type) {
        case JoinClause::INNER: return JoinType::INNER;
        case JoinClause::LEFT: return JoinType::LEFT_OUTER;
        case JoinClause::RIGHT: return JoinType::RIGHT_OUTER;
        default: return JoinType::INNER;
    }
}

std::unique_ptr<PlanNode> PlanBuilder::build_scan_node(const TableReference& table) {
    auto scan = std::make_unique<TableScanNode>(table.table_name, table.alias);
    
    auto stats_it = table_stats.find(table.table_name);
    if (stats_it != table_stats.end()) {
        scan->stats = stats_it->second;
    }
    
    scan->output_schema.add_column(Column(table.table_name, "*"));
    
    return std::move(scan);
}

std::unique_ptr<PlanNode> PlanBuilder::build_filter_node(std::unique_ptr<PlanNode> child, const Expression& condition) {
    auto filter = std::make_unique<FilterNode>(expression_to_string(condition));
    
    filter->stats = child->stats;
    filter->stats.selectivity = 0.1;
    filter->stats.row_count = static_cast<size_t>(child->stats.row_count * filter->stats.selectivity);
    
    filter->output_schema = child->output_schema;
    filter->children.push_back(std::move(child));
    
    return std::move(filter);
}

std::unique_ptr<PlanNode> PlanBuilder::build_project_node(std::unique_ptr<PlanNode> child, const std::vector<SelectItem>& items) {
    std::vector<std::string> projections;
    for (const auto& item : items) {
        projections.push_back(expression_to_string(*item.expression));
    }
    
    auto project = std::make_unique<ProjectNode>(projections);
    project->stats = child->stats;
    project->output_schema = child->output_schema;
    project->children.push_back(std::move(child));
    
    return std::move(project);
}

std::unique_ptr<PlanNode> PlanBuilder::build_join_node(std::unique_ptr<PlanNode> left, std::unique_ptr<PlanNode> right, 
                                                     const JoinClause& join, PlanNodeType join_algorithm) {
    std::unique_ptr<JoinNode> join_node;
    JoinType join_type = convert_join_type(join.join_type);
    std::string condition = expression_to_string(*join.condition);
    
    switch (join_algorithm) {
        case PlanNodeType::NESTED_LOOP_JOIN:
            join_node = std::make_unique<NestedLoopJoinNode>(join_type, condition);
            break;
        case PlanNodeType::HASH_JOIN:
            join_node = std::make_unique<HashJoinNode>(join_type, condition);
            break;
        case PlanNodeType::SORT_MERGE_JOIN:
            join_node = std::make_unique<SortMergeJoinNode>(join_type, condition);
            break;
        default:
            join_node = std::make_unique<NestedLoopJoinNode>(join_type, condition);
    }
    
    join_node->stats.row_count = left->stats.row_count * right->stats.row_count / 10;
    join_node->stats.page_count = join_node->stats.row_count / 100;
    join_node->stats.selectivity = 0.1;
    
    join_node->output_schema = left->output_schema;
    for (const auto& col : right->output_schema.columns) {
        join_node->output_schema.add_column(col);
    }
    
    join_node->children.push_back(std::move(left));
    join_node->children.push_back(std::move(right));
    
    return std::move(join_node);
}

std::unique_ptr<PlanNode> PlanBuilder::build_plan(const SelectStatement& stmt) {
    auto plan = build_scan_node(stmt.from_table);
    
    for (const auto& join : stmt.joins) {
        auto right_plan = build_scan_node(join.table);
        plan = build_join_node(std::move(plan), std::move(right_plan), join, PlanNodeType::NESTED_LOOP_JOIN);
    }
    
    if (stmt.where_clause) {
        plan = build_filter_node(std::move(plan), *stmt.where_clause);
    }
    
    plan = build_project_node(std::move(plan), stmt.select_list);
    
    return plan;
}

std::vector<std::unique_ptr<PlanNode>> PlanBuilder::generate_join_orders(const SelectStatement& stmt) {
    std::vector<std::unique_ptr<PlanNode>> plans;
    
    if (stmt.joins.empty()) {
        plans.push_back(build_plan(stmt));
        return plans;
    }
    
    std::vector<PlanNodeType> join_algorithms = {
        PlanNodeType::NESTED_LOOP_JOIN,
        PlanNodeType::HASH_JOIN,
        PlanNodeType::SORT_MERGE_JOIN
    };
    
    for (auto algorithm : join_algorithms) {
        auto plan = build_scan_node(stmt.from_table);
        
        for (const auto& join : stmt.joins) {
            auto right_plan = build_scan_node(join.table);
            plan = build_join_node(std::move(plan), std::move(right_plan), join, algorithm);
        }
        
        if (stmt.where_clause) {
            plan = build_filter_node(std::move(plan), *stmt.where_clause);
        }
        
        plan = build_project_node(std::move(plan), stmt.select_list);
        plans.push_back(std::move(plan));
    }
    
    return plans;
}

std::unique_ptr<PlanNode> PlanBuilder::optimize_plan(std::unique_ptr<PlanNode> plan) {
    plan->cost = plan->estimate_cost();
    return plan;
}