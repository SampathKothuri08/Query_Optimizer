#pragma once
#include "query_plan.h"
#include "table.h"
#include <vector>
#include <memory>
#include <iostream>

class ResultSet {
private:
    TableSchema schema;
    std::vector<Row> rows;
    
public:
    ResultSet(const TableSchema& result_schema) : schema(result_schema) {}
    
    void add_row(const Row& row) {
        rows.push_back(row);
    }
    
    const std::vector<Row>& get_rows() const {
        return rows;
    }
    
    const TableSchema& get_schema() const {
        return schema;
    }
    
    size_t size() const {
        return rows.size();
    }
    
    void print(size_t limit = 10) const {
        std::cout << "Result (" << rows.size() << " rows):" << std::endl;
        
        for (size_t i = 0; i < schema.column_names.size(); ++i) {
            std::cout << schema.column_names[i];
            if (i < schema.column_names.size() - 1) std::cout << "\t";
        }
        std::cout << std::endl;
        
        for (size_t i = 0; i < std::min(rows.size(), limit); ++i) {
            const auto& row = rows[i];
            for (size_t j = 0; j < row.size() && j < schema.column_names.size(); ++j) {
                try {
                    if (schema.column_types[j] == "int") {
                        std::cout << row.get<int>(j);
                    } else {
                        std::cout << row.get<std::string>(j);
                    }
                } catch (...) {
                    std::cout << "NULL";
                }
                if (j < row.size() - 1) std::cout << "\t";
            }
            std::cout << std::endl;
        }
        
        if (rows.size() > limit) {
            std::cout << "... (" << (rows.size() - limit) << " more rows)" << std::endl;
        }
    }
};

class Executor {
private:
    TableManager* table_manager;
    
    std::unique_ptr<ResultSet> execute_table_scan(const TableScanNode& node);
    std::unique_ptr<ResultSet> execute_filter(const FilterNode& node);
    std::unique_ptr<ResultSet> execute_project(const ProjectNode& node);
    std::unique_ptr<ResultSet> execute_nested_loop_join(const NestedLoopJoinNode& node);
    std::unique_ptr<ResultSet> execute_hash_join(const HashJoinNode& node);
    std::unique_ptr<ResultSet> execute_sort_merge_join(const SortMergeJoinNode& node);
    
    bool evaluate_condition(const Row& row, const TableSchema& schema, const std::string& condition);
    std::vector<std::string> parse_projections(const std::vector<std::string>& projections);
    
public:
    Executor(TableManager* tm) : table_manager(tm) {}
    
    std::unique_ptr<ResultSet> execute(const PlanNode& node);
};