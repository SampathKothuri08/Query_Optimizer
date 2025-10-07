#pragma once
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>

struct Statistics {
    size_t row_count;
    size_t page_count;
    double selectivity;
    
    Statistics(size_t rows = 0, size_t pages = 0, double sel = 1.0)
        : row_count(rows), page_count(pages), selectivity(sel) {}
};

struct CostEstimate {
    double io_cost;
    double cpu_cost;
    double total_cost;
    
    CostEstimate(double io = 0.0, double cpu = 0.0)
        : io_cost(io), cpu_cost(cpu), total_cost(io + cpu) {}
};

enum class PlanNodeType {
    TABLE_SCAN,
    INDEX_SCAN,
    FILTER,
    PROJECT,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    SORT_MERGE_JOIN,
    SORT,
    AGGREGATE
};

struct Column {
    std::string table_name;
    std::string column_name;
    std::string data_type;
    
    Column(const std::string& table, const std::string& column, const std::string& type = "int")
        : table_name(table), column_name(column), data_type(type) {}
        
    std::string full_name() const {
        return table_name.empty() ? column_name : table_name + "." + column_name;
    }
};

struct Schema {
    std::vector<Column> columns;
    
    void add_column(const Column& col) {
        columns.push_back(col);
    }
    
    size_t column_count() const {
        return columns.size();
    }
};

class PlanNode {
public:
    PlanNodeType type;
    Schema output_schema;
    Statistics stats;
    CostEstimate cost;
    std::vector<std::unique_ptr<PlanNode>> children;
    
    PlanNode(PlanNodeType node_type) : type(node_type) {}
    virtual ~PlanNode() = default;
    
    virtual std::string to_string(int indent = 0) const = 0;
    virtual CostEstimate estimate_cost() = 0;
    
protected:
    std::string indent_string(int indent) const {
        return std::string(indent * 2, ' ');
    }
};

class TableScanNode : public PlanNode {
public:
    std::string table_name;
    std::string alias;
    
    TableScanNode(const std::string& table, const std::string& table_alias = "")
        : PlanNode(PlanNodeType::TABLE_SCAN), table_name(table), alias(table_alias) {}
    
    std::string to_string(int indent = 0) const override {
        return indent_string(indent) + "TableScan(" + table_name + 
               (alias.empty() ? "" : " as " + alias) + ")";
    }
    
    CostEstimate estimate_cost() override {
        return CostEstimate(stats.page_count, stats.row_count * 0.01);
    }
};

class FilterNode : public PlanNode {
public:
    std::string condition;
    
    FilterNode(const std::string& filter_condition)
        : PlanNode(PlanNodeType::FILTER), condition(filter_condition) {}
    
    std::string to_string(int indent = 0) const override {
        std::string result = indent_string(indent) + "Filter(" + condition + ")\n";
        if (!children.empty()) {
            result += children[0]->to_string(indent + 1);
        }
        return result;
    }
    
    CostEstimate estimate_cost() override {
        if (children.empty()) return CostEstimate();
        auto child_cost = children[0]->estimate_cost();
        return CostEstimate(child_cost.io_cost, child_cost.cpu_cost + stats.row_count * 0.02);
    }
};

class ProjectNode : public PlanNode {
public:
    std::vector<std::string> projection_list;
    
    ProjectNode(const std::vector<std::string>& projections)
        : PlanNode(PlanNodeType::PROJECT), projection_list(projections) {}
    
    std::string to_string(int indent = 0) const override {
        std::string result = indent_string(indent) + "Project(";
        for (size_t i = 0; i < projection_list.size(); ++i) {
            if (i > 0) result += ", ";
            result += projection_list[i];
        }
        result += ")\n";
        if (!children.empty()) {
            result += children[0]->to_string(indent + 1);
        }
        return result;
    }
    
    CostEstimate estimate_cost() override {
        if (children.empty()) return CostEstimate();
        auto child_cost = children[0]->estimate_cost();
        return CostEstimate(child_cost.io_cost, child_cost.cpu_cost + stats.row_count * 0.01);
    }
};

enum class JoinType {
    INNER,
    LEFT_OUTER,
    RIGHT_OUTER,
    FULL_OUTER
};

class JoinNode : public PlanNode {
public:
    JoinType join_type;
    std::string join_condition;
    
    JoinNode(PlanNodeType node_type, JoinType type, const std::string& condition)
        : PlanNode(node_type), join_type(type), join_condition(condition) {}
    
    virtual ~JoinNode() = default;
    
protected:
    std::string join_type_string() const {
        switch (join_type) {
            case JoinType::INNER: return "INNER";
            case JoinType::LEFT_OUTER: return "LEFT";
            case JoinType::RIGHT_OUTER: return "RIGHT";
            case JoinType::FULL_OUTER: return "FULL";
            default: return "UNKNOWN";
        }
    }
};

class NestedLoopJoinNode : public JoinNode {
public:
    NestedLoopJoinNode(JoinType type, const std::string& condition)
        : JoinNode(PlanNodeType::NESTED_LOOP_JOIN, type, condition) {}
    
    std::string to_string(int indent = 0) const override {
        std::string result = indent_string(indent) + "NestedLoopJoin(" + 
                           join_type_string() + ", " + join_condition + ")\n";
        if (children.size() >= 2) {
            result += children[0]->to_string(indent + 1) + "\n";
            result += children[1]->to_string(indent + 1);
        }
        return result;
    }
    
    CostEstimate estimate_cost() override {
        if (children.size() < 2) return CostEstimate();
        auto left_cost = children[0]->estimate_cost();
        auto right_cost = children[1]->estimate_cost();
        
        double io_cost = left_cost.io_cost + (children[0]->stats.row_count * right_cost.io_cost);
        double cpu_cost = left_cost.cpu_cost + right_cost.cpu_cost + 
                         (children[0]->stats.row_count * children[1]->stats.row_count * 0.01);
        
        return CostEstimate(io_cost, cpu_cost);
    }
};

class HashJoinNode : public JoinNode {
public:
    HashJoinNode(JoinType type, const std::string& condition)
        : JoinNode(PlanNodeType::HASH_JOIN, type, condition) {}
    
    std::string to_string(int indent = 0) const override {
        std::string result = indent_string(indent) + "HashJoin(" + 
                           join_type_string() + ", " + join_condition + ")\n";
        if (children.size() >= 2) {
            result += children[0]->to_string(indent + 1) + "\n";
            result += children[1]->to_string(indent + 1);
        }
        return result;
    }
    
    CostEstimate estimate_cost() override {
        if (children.size() < 2) return CostEstimate();
        auto left_cost = children[0]->estimate_cost();
        auto right_cost = children[1]->estimate_cost();
        
        double io_cost = left_cost.io_cost + right_cost.io_cost;
        double cpu_cost = left_cost.cpu_cost + right_cost.cpu_cost + 
                         (children[0]->stats.row_count + children[1]->stats.row_count) * 0.02;
        
        return CostEstimate(io_cost, cpu_cost);
    }
};

class SortMergeJoinNode : public JoinNode {
public:
    SortMergeJoinNode(JoinType type, const std::string& condition)
        : JoinNode(PlanNodeType::SORT_MERGE_JOIN, type, condition) {}
    
    std::string to_string(int indent = 0) const override {
        std::string result = indent_string(indent) + "SortMergeJoin(" + 
                           join_type_string() + ", " + join_condition + ")\n";
        if (children.size() >= 2) {
            result += children[0]->to_string(indent + 1) + "\n";
            result += children[1]->to_string(indent + 1);
        }
        return result;
    }
    
    CostEstimate estimate_cost() override {
        if (children.size() < 2) return CostEstimate();
        auto left_cost = children[0]->estimate_cost();
        auto right_cost = children[1]->estimate_cost();
        
        double left_sort_cost = children[0]->stats.row_count * std::log2(children[0]->stats.row_count) * 0.01;
        double right_sort_cost = children[1]->stats.row_count * std::log2(children[1]->stats.row_count) * 0.01;
        
        double io_cost = left_cost.io_cost + right_cost.io_cost;
        double cpu_cost = left_cost.cpu_cost + right_cost.cpu_cost + left_sort_cost + right_sort_cost;
        
        return CostEstimate(io_cost, cpu_cost);
    }
};