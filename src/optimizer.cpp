#include "optimizer.h"
#include <algorithm>
#include <iostream>

QueryOptimizer::QueryOptimizer() {
    cost_model.set_table_statistics("users", TableStatistics(1000, 10, 120));
    cost_model.set_table_statistics("orders", TableStatistics(5000, 50, 80));
}

void QueryOptimizer::set_table_statistics(const std::string& table_name, const TableStatistics& stats) {
    cost_model.set_table_statistics(table_name, stats);
    plan_builder.set_table_statistics(table_name, Statistics(stats.tuple_count, stats.page_count, 1.0));
}

std::unique_ptr<PlanNode> QueryOptimizer::optimize_single_table(const SelectStatement& stmt) {
    auto plan = plan_builder.build_plan(stmt);
    
    plan = apply_filter_pushdown(std::move(plan));
    
    auto cost = cost_model.estimate_plan_cost(*plan);
    plan->cost = cost;
    
    return plan;
}

std::unique_ptr<PlanNode> QueryOptimizer::optimize_join_query(const SelectStatement& stmt) {
    auto candidates = generate_all_plans(stmt);
    
    if (candidates.empty()) {
        return nullptr;
    }
    
    return select_best_plan(candidates);
}

std::unique_ptr<PlanNode> QueryOptimizer::optimize(const SelectStatement& stmt) {
    if (stmt.joins.empty()) {
        return optimize_single_table(stmt);
    } else {
        return optimize_join_query(stmt);
    }
}

std::vector<QueryOptimizer::PlanCandidate> QueryOptimizer::generate_all_plans(const SelectStatement& stmt) {
    std::vector<PlanCandidate> candidates;
    
    std::vector<PlanNodeType> join_algorithms = {
        PlanNodeType::NESTED_LOOP_JOIN,
        PlanNodeType::HASH_JOIN,
        PlanNodeType::SORT_MERGE_JOIN
    };
    
    for (auto algorithm : join_algorithms) {
        try {
            auto plan = plan_builder.build_scan_node(stmt.from_table);
            
            for (const auto& join : stmt.joins) {
                auto right_plan = plan_builder.build_scan_node(join.table);
                plan = plan_builder.build_join_node(std::move(plan), std::move(right_plan), join, algorithm);
            }
            
            if (stmt.where_clause) {
                plan = plan_builder.build_filter_node(std::move(plan), *stmt.where_clause);
            }
            
            plan = plan_builder.build_project_node(std::move(plan), stmt.select_list);
            
            auto cost = cost_model.estimate_plan_cost(*plan);
            plan->cost = cost;
            
            candidates.emplace_back(std::move(plan), cost);
            
        } catch (const std::exception& e) {
            std::cerr << "Error generating plan with algorithm " << static_cast<int>(algorithm) 
                      << ": " << e.what() << std::endl;
        }
    }
    
    if (stmt.joins.size() == 1) {
        try {
            auto right_first = plan_builder.build_scan_node(stmt.joins[0].table);
            auto left_second = plan_builder.build_scan_node(stmt.from_table);
            
            for (auto algorithm : join_algorithms) {
                auto plan = plan_builder.build_join_node(std::move(right_first), std::move(left_second), 
                                                       stmt.joins[0], algorithm);
                
                if (stmt.where_clause) {
                    plan = plan_builder.build_filter_node(std::move(plan), *stmt.where_clause);
                }
                
                plan = plan_builder.build_project_node(std::move(plan), stmt.select_list);
                
                auto cost = cost_model.estimate_plan_cost(*plan);
                plan->cost = cost;
                
                candidates.emplace_back(std::move(plan), cost);
                
                right_first = plan_builder.build_scan_node(stmt.joins[0].table);
                left_second = plan_builder.build_scan_node(stmt.from_table);
            }
        } catch (const std::exception& e) {
            std::cerr << "Error generating reversed join plan: " << e.what() << std::endl;
        }
    }
    
    return candidates;
}

std::unique_ptr<PlanNode> QueryOptimizer::select_best_plan(std::vector<PlanCandidate>& candidates) {
    if (candidates.empty()) {
        return nullptr;
    }
    
    auto best_it = std::min_element(candidates.begin(), candidates.end(),
        [](const PlanCandidate& a, const PlanCandidate& b) {
            return a.cost.total_cost < b.cost.total_cost;
        });
    
    return std::move(best_it->plan);
}

std::unique_ptr<PlanNode> QueryOptimizer::apply_filter_pushdown(std::unique_ptr<PlanNode> plan) {
    return plan;
}

std::unique_ptr<PlanNode> QueryOptimizer::choose_join_algorithm(std::unique_ptr<PlanNode> plan) {
    return plan;
}

void QueryOptimizer::print_optimization_report(const std::vector<PlanCandidate>& candidates) {
    std::cout << "\n=== Query Optimization Report ===" << std::endl;
    std::cout << "Generated " << candidates.size() << " plan alternatives:" << std::endl;
    
    for (size_t i = 0; i < candidates.size(); ++i) {
        std::cout << "\nPlan " << (i + 1) << ":" << std::endl;
        std::cout << candidates[i].plan->to_string() << std::endl;
        std::cout << "Cost: I/O=" << candidates[i].cost.io_cost 
                  << ", CPU=" << candidates[i].cost.cpu_cost 
                  << ", Total=" << candidates[i].cost.total_cost << std::endl;
    }
    
    auto best_it = std::min_element(candidates.begin(), candidates.end(),
        [](const PlanCandidate& a, const PlanCandidate& b) {
            return a.cost.total_cost < b.cost.total_cost;
        });
    
    if (best_it != candidates.end()) {
        size_t best_index = std::distance(candidates.begin(), best_it);
        std::cout << "\n*** SELECTED PLAN " << (best_index + 1) 
                  << " (Lowest Cost: " << best_it->cost.total_cost << ") ***" << std::endl;
    }
}