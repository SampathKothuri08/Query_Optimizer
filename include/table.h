#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <any>

struct Row {
    std::vector<std::any> values;
    
    template<typename T>
    T get(size_t index) const {
        if (index >= values.size()) {
            throw std::out_of_range("Column index out of range");
        }
        return std::any_cast<T>(values[index]);
    }
    
    template<typename T>
    void set(size_t index, const T& value) {
        if (index >= values.size()) {
            values.resize(index + 1);
        }
        values[index] = value;
    }
    
    void add_value(const std::any& value) {
        values.push_back(value);
    }
    
    size_t size() const {
        return values.size();
    }
};

struct TableSchema {
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
    
    void add_column(const std::string& name, const std::string& type) {
        column_names.push_back(name);
        column_types.push_back(type);
    }
    
    size_t get_column_index(const std::string& name) const {
        for (size_t i = 0; i < column_names.size(); ++i) {
            if (column_names[i] == name) {
                return i;
            }
        }
        throw std::runtime_error("Column not found: " + name);
    }
    
    size_t column_count() const {
        return column_names.size();
    }
};

class Table {
private:
    std::string name;
    TableSchema schema;
    std::vector<Row> rows;
    
public:
    Table(const std::string& table_name) : name(table_name) {}
    
    void set_schema(const TableSchema& table_schema) {
        schema = table_schema;
    }
    
    void add_row(const Row& row) {
        rows.push_back(row);
    }
    
    const std::vector<Row>& get_rows() const {
        return rows;
    }
    
    const TableSchema& get_schema() const {
        return schema;
    }
    
    const std::string& get_name() const {
        return name;
    }
    
    size_t row_count() const {
        return rows.size();
    }
    
    void clear() {
        rows.clear();
    }
};

class TableManager {
private:
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;
    
public:
    void create_table(const std::string& name, const TableSchema& schema) {
        auto table = std::make_unique<Table>(name);
        table->set_schema(schema);
        tables[name] = std::move(table);
    }
    
    Table* get_table(const std::string& name) {
        auto it = tables.find(name);
        return (it != tables.end()) ? it->second.get() : nullptr;
    }
    
    void populate_sample_data() {
        {
            TableSchema users_schema;
            users_schema.add_column("id", "int");
            users_schema.add_column("name", "string");
            users_schema.add_column("age", "int");
            users_schema.add_column("city", "string");
            
            create_table("users", users_schema);
            auto users = get_table("users");
            
            for (int i = 1; i <= 1000; ++i) {
                Row row;
                row.add_value(i);
                row.add_value(std::string("User") + std::to_string(i));
                row.add_value(20 + (i % 50));
                row.add_value(std::string("City") + std::to_string((i % 10) + 1));
                users->add_row(row);
            }
        }
        
        {
            TableSchema orders_schema;
            orders_schema.add_column("id", "int");
            orders_schema.add_column("user_id", "int");
            orders_schema.add_column("product", "string");
            orders_schema.add_column("amount", "int");
            
            create_table("orders", orders_schema);
            auto orders = get_table("orders");
            
            for (int i = 1; i <= 5000; ++i) {
                Row row;
                row.add_value(i);
                row.add_value((i % 1000) + 1);
                row.add_value(std::string("Product") + std::to_string((i % 100) + 1));
                row.add_value(10 + (i % 500));
                orders->add_row(row);
            }
        }
    }
};