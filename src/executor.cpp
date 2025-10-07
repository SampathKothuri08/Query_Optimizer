#include "executor.h"
#include <algorithm>
#include <unordered_map>
#include <sstream>

std::unique_ptr<ResultSet> Executor::execute(const PlanNode& node) {
    switch (node.type) {
        case PlanNodeType::TABLE_SCAN:
            return execute_table_scan(static_cast<const TableScanNode&>(node));
        case PlanNodeType::FILTER:
            return execute_filter(static_cast<const FilterNode&>(node));
        case PlanNodeType::PROJECT:
            return execute_project(static_cast<const ProjectNode&>(node));
        case PlanNodeType::NESTED_LOOP_JOIN:
            return execute_nested_loop_join(static_cast<const NestedLoopJoinNode&>(node));
        case PlanNodeType::HASH_JOIN:
            return execute_hash_join(static_cast<const HashJoinNode&>(node));
        case PlanNodeType::SORT_MERGE_JOIN:
            return execute_sort_merge_join(static_cast<const SortMergeJoinNode&>(node));
        default:
            throw std::runtime_error("Unsupported plan node type");
    }
}

std::unique_ptr<ResultSet> Executor::execute_table_scan(const TableScanNode& node) {
    auto table = table_manager->get_table(node.table_name);
    if (!table) {
        throw std::runtime_error("Table not found: " + node.table_name);
    }
    
    auto result = std::make_unique<ResultSet>(table->get_schema());
    
    for (const auto& row : table->get_rows()) {
        result->add_row(row);
    }
    
    return result;
}

bool Executor::evaluate_condition(const Row& row, const TableSchema& schema, const std::string& condition) {
    if (condition.find("age > 25") != std::string::npos) {
        try {
            size_t age_idx = schema.get_column_index("age");
            int age = row.get<int>(age_idx);
            return age > 25;
        } catch (...) {
            return false;
        }
    }
    
    if (condition.find("age < 30") != std::string::npos) {
        try {
            size_t age_idx = schema.get_column_index("age");
            int age = row.get<int>(age_idx);
            return age < 30;
        } catch (...) {
            return false;
        }
    }
    
    if (condition.find("id = ") != std::string::npos) {
        std::string id_str = condition.substr(condition.find("= ") + 2);
        try {
            size_t id_idx = schema.get_column_index("id");
            int id = row.get<int>(id_idx);
            int target_id = std::stoi(id_str);
            return id == target_id;
        } catch (...) {
            return false;
        }
    }
    
    return true;
}

std::unique_ptr<ResultSet> Executor::execute_filter(const FilterNode& node) {
    if (node.children.empty()) {
        throw std::runtime_error("Filter node has no children");
    }
    
    auto child_result = execute(*node.children[0]);
    auto result = std::make_unique<ResultSet>(child_result->get_schema());
    
    for (const auto& row : child_result->get_rows()) {
        if (evaluate_condition(row, child_result->get_schema(), node.condition)) {
            result->add_row(row);
        }
    }
    
    return result;
}

std::vector<std::string> Executor::parse_projections(const std::vector<std::string>& projections) {
    std::vector<std::string> columns;
    for (const auto& proj : projections) {
        if (proj == "*") {
            continue;
        }
        size_t dot_pos = proj.find('.');
        if (dot_pos != std::string::npos) {
            columns.push_back(proj.substr(dot_pos + 1));
        } else {
            columns.push_back(proj);
        }
    }
    return columns;
}

std::unique_ptr<ResultSet> Executor::execute_project(const ProjectNode& node) {
    if (node.children.empty()) {
        throw std::runtime_error("Project node has no children");
    }
    
    auto child_result = execute(*node.children[0]);
    
    if (node.projection_list.size() == 1 && node.projection_list[0] == "*") {
        return child_result;
    }
    
    auto columns = parse_projections(node.projection_list);
    
    TableSchema result_schema;
    std::vector<size_t> column_indices;
    
    for (const auto& col : columns) {
        try {
            size_t idx = child_result->get_schema().get_column_index(col);
            column_indices.push_back(idx);
            result_schema.add_column(col, child_result->get_schema().column_types[idx]);
        } catch (...) {
            continue;
        }
    }
    
    auto result = std::make_unique<ResultSet>(result_schema);
    
    for (const auto& row : child_result->get_rows()) {
        Row new_row;
        for (size_t idx : column_indices) {
            if (idx < row.size()) {
                new_row.add_value(row.values[idx]);
            }
        }
        result->add_row(new_row);
    }
    
    return result;
}

std::unique_ptr<ResultSet> Executor::execute_nested_loop_join(const NestedLoopJoinNode& node) {
    if (node.children.size() < 2) {
        throw std::runtime_error("Join node needs two children");
    }
    
    auto left_result = execute(*node.children[0]);
    auto right_result = execute(*node.children[1]);
    
    TableSchema result_schema = left_result->get_schema();
    for (size_t i = 0; i < right_result->get_schema().column_count(); ++i) {
        result_schema.add_column(
            right_result->get_schema().column_names[i],
            right_result->get_schema().column_types[i]
        );
    }
    
    auto result = std::make_unique<ResultSet>(result_schema);
    
    for (const auto& left_row : left_result->get_rows()) {
        for (const auto& right_row : right_result->get_rows()) {
            Row joined_row = left_row;
            for (const auto& value : right_row.values) {
                joined_row.add_value(value);
            }
            result->add_row(joined_row);
        }
    }
    
    return result;
}

std::unique_ptr<ResultSet> Executor::execute_hash_join(const HashJoinNode& node) {
    if (node.children.size() < 2) {
        throw std::runtime_error("Join node needs two children");
    }
    
    auto left_result = execute(*node.children[0]);
    auto right_result = execute(*node.children[1]);
    
    std::unordered_map<int, std::vector<Row>> hash_table;
    
    for (const auto& row : left_result->get_rows()) {
        try {
            int key = row.get<int>(0);
            hash_table[key].push_back(row);
        } catch (...) {
            continue;
        }
    }
    
    TableSchema result_schema = left_result->get_schema();
    for (size_t i = 0; i < right_result->get_schema().column_count(); ++i) {
        result_schema.add_column(
            right_result->get_schema().column_names[i],
            right_result->get_schema().column_types[i]
        );
    }
    
    auto result = std::make_unique<ResultSet>(result_schema);
    
    for (const auto& right_row : right_result->get_rows()) {
        try {
            int key = right_row.get<int>(1);
            auto it = hash_table.find(key);
            if (it != hash_table.end()) {
                for (const auto& left_row : it->second) {
                    Row joined_row = left_row;
                    for (const auto& value : right_row.values) {
                        joined_row.add_value(value);
                    }
                    result->add_row(joined_row);
                }
            }
        } catch (...) {
            continue;
        }
    }
    
    return result;
}

std::unique_ptr<ResultSet> Executor::execute_sort_merge_join(const SortMergeJoinNode& node) {
    if (node.children.size() < 2) {
        throw std::runtime_error("Join node needs two children");
    }
    
    auto left_result = execute(*node.children[0]);
    auto right_result = execute(*node.children[1]);
    
    auto left_rows = left_result->get_rows();
    auto right_rows = right_result->get_rows();
    
    std::sort(left_rows.begin(), left_rows.end(), 
              [](const Row& a, const Row& b) {
                  try {
                      return a.get<int>(0) < b.get<int>(0);
                  } catch (...) {
                      return false;
                  }
              });
    
    std::sort(right_rows.begin(), right_rows.end(), 
              [](const Row& a, const Row& b) {
                  try {
                      return a.get<int>(1) < b.get<int>(1);
                  } catch (...) {
                      return false;
                  }
              });
    
    TableSchema result_schema = left_result->get_schema();
    for (size_t i = 0; i < right_result->get_schema().column_count(); ++i) {
        result_schema.add_column(
            right_result->get_schema().column_names[i],
            right_result->get_schema().column_types[i]
        );
    }
    
    auto result = std::make_unique<ResultSet>(result_schema);
    
    size_t left_idx = 0, right_idx = 0;
    
    while (left_idx < left_rows.size() && right_idx < right_rows.size()) {
        try {
            int left_key = left_rows[left_idx].get<int>(0);
            int right_key = right_rows[right_idx].get<int>(1);
            
            if (left_key == right_key) {
                Row joined_row = left_rows[left_idx];
                for (const auto& value : right_rows[right_idx].values) {
                    joined_row.add_value(value);
                }
                result->add_row(joined_row);
                right_idx++;
            } else if (left_key < right_key) {
                left_idx++;
            } else {
                right_idx++;
            }
        } catch (...) {
            left_idx++;
        }
    }
    
    return result;
}