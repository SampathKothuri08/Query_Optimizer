#include "benchmark.h"
#include <iostream>
#include <fstream>
#include <random>
#include <iomanip>

void DataGenerator::generate_large_dataset(TableManager& tm, size_t users_count, size_t orders_count) {
    TableSchema users_schema;
    users_schema.add_column("id", "int");
    users_schema.add_column("name", "string");
    users_schema.add_column("age", "int");
    users_schema.add_column("city", "string");
    
    tm.create_table("users", users_schema);
    auto users = tm.get_table("users");
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> age_dist(18, 65);
    std::uniform_int_distribution<> city_dist(1, 20);
    
    for (size_t i = 1; i <= users_count; ++i) {
        Row row;
        row.add_value(static_cast<int>(i));
        row.add_value(std::string("User") + std::to_string(i));
        row.add_value(age_dist(gen));
        row.add_value(std::string("City") + std::to_string(city_dist(gen)));
        users->add_row(row);
    }
    
    TableSchema orders_schema;
    orders_schema.add_column("id", "int");
    orders_schema.add_column("user_id", "int");
    orders_schema.add_column("product", "string");
    orders_schema.add_column("amount", "int");
    
    tm.create_table("orders", orders_schema);
    auto orders = tm.get_table("orders");
    
    std::uniform_int_distribution<> user_dist(1, static_cast<int>(users_count));
    std::uniform_int_distribution<> product_dist(1, 200);
    std::uniform_int_distribution<> amount_dist(10, 1000);
    
    for (size_t i = 1; i <= orders_count; ++i) {
        Row row;
        row.add_value(static_cast<int>(i));
        row.add_value(user_dist(gen));
        row.add_value(std::string("Product") + std::to_string(product_dist(gen)));
        row.add_value(amount_dist(gen));
        orders->add_row(row);
    }
}

void DataGenerator::generate_skewed_dataset(TableManager& tm, size_t users_count, size_t orders_count) {
    generate_large_dataset(tm, users_count, orders_count);
    
    auto orders = tm.get_table("orders");
    orders->clear();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> product_dist(1, 200);
    std::uniform_int_distribution<> amount_dist(10, 1000);
    
    for (size_t i = 1; i <= orders_count; ++i) {
        Row row;
        row.add_value(static_cast<int>(i));
        
        int user_id;
        if (i <= orders_count * 0.8) {
            user_id = (i % 10) + 1;
        } else {
            std::uniform_int_distribution<> user_dist(11, static_cast<int>(users_count));
            user_id = user_dist(gen);
        }
        
        row.add_value(user_id);
        row.add_value(std::string("Product") + std::to_string(product_dist(gen)));
        row.add_value(amount_dist(gen));
        orders->add_row(row);
    }
}

void DataGenerator::generate_uniform_dataset(TableManager& tm, size_t users_count, size_t orders_count) {
    generate_large_dataset(tm, users_count, orders_count);
}

QueryBenchmark::QueryBenchmark(TableManager* tm) 
    : table_manager(tm), executor(tm) {}

double QueryBenchmark::measure_execution_time(const PlanNode& plan) {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto result = executor.execute(plan);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    return duration.count() / 1000.0;
}

void QueryBenchmark::run_single_table_benchmarks() {
    std::cout << "\n=== Single Table Benchmarks ===" << std::endl;
    
    SelectStatement stmt;
    stmt.from_table = TableReference("users");
    
    SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
    stmt.select_list.push_back(std::move(all_item));
    
    std::vector<std::string> conditions = {
        "age > 25",
        "age < 30", 
        "age > 40"
    };
    
    for (const auto& condition : conditions) {
        SelectStatement filter_stmt;
        filter_stmt.from_table = stmt.from_table;
        
        SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
        filter_stmt.select_list.push_back(std::move(all_item));
        
        if (condition == "age > 25") {
            filter_stmt.where_clause = std::make_unique<BinaryOpExpression>(
                std::make_unique<ColumnExpression>("", "age"),
                std::make_unique<LiteralExpression>("25"),
                BinaryOperator::GREATER
            );
        }
        
        auto plan = optimizer.optimize(filter_stmt);
        double exec_time = measure_execution_time(*plan);
        
        auto result = executor.execute(*plan);
        
        results.emplace_back("SingleTable_" + condition, "Optimized", 
                           exec_time, plan->cost.total_cost, result->size());
        
        std::cout << "Query: " << condition 
                  << " | Time: " << std::fixed << std::setprecision(2) << exec_time << "ms"
                  << " | Results: " << result->size() << std::endl;
    }
}

void QueryBenchmark::compare_join_algorithms(const SelectStatement& stmt) {
    std::cout << "\n=== Join Algorithm Comparison ===" << std::endl;
    
    std::vector<std::pair<PlanNodeType, std::string>> algorithms = {
        {PlanNodeType::NESTED_LOOP_JOIN, "NestedLoop"},
        {PlanNodeType::HASH_JOIN, "HashJoin"},
        {PlanNodeType::SORT_MERGE_JOIN, "SortMerge"}
    };
    
    for (const auto& [algo_type, algo_name] : algorithms) {
        try {
            PlanBuilder builder;
            auto plan = builder.build_scan_node(stmt.from_table);
            
            for (const auto& join : stmt.joins) {
                auto right_plan = builder.build_scan_node(join.table);
                plan = builder.build_join_node(std::move(plan), std::move(right_plan), join, algo_type);
            }
            
            SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
            std::vector<SelectItem> select_list;
            select_list.push_back(std::move(all_item));
            plan = builder.build_project_node(std::move(plan), select_list);
            
            CostModel cost_model;
            auto cost = cost_model.estimate_plan_cost(*plan);
            plan->cost = cost;
            
            double exec_time = measure_execution_time(*plan);
            auto result = executor.execute(*plan);
            
            results.emplace_back("Join", algo_name, exec_time, cost.total_cost, result->size());
            
            std::cout << algo_name 
                      << " | Time: " << std::fixed << std::setprecision(2) << exec_time << "ms"
                      << " | Cost: " << cost.total_cost
                      << " | Results: " << result->size() << std::endl;
                      
        } catch (const std::exception& e) {
            std::cout << algo_name << " | ERROR: " << e.what() << std::endl;
        }
    }
}

void QueryBenchmark::run_join_benchmarks() {
    SelectStatement join_stmt;
    join_stmt.from_table = TableReference("users");
    
    JoinClause join(JoinClause::INNER, TableReference("orders"), 
                   std::make_unique<BinaryOpExpression>(
                       std::make_unique<ColumnExpression>("users", "id"),
                       std::make_unique<ColumnExpression>("orders", "user_id"),
                       BinaryOperator::EQUALS
                   ));
    join_stmt.joins.push_back(std::move(join));
    
    SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
    join_stmt.select_list.push_back(std::move(all_item));
    
    compare_join_algorithms(join_stmt);
}

void QueryBenchmark::run_scalability_benchmarks() {
    std::cout << "\n=== Scalability Benchmarks ===" << std::endl;
    
    std::vector<std::pair<size_t, size_t>> dataset_sizes = {
        {100, 500},
        {500, 2500}, 
        {1000, 5000},
        {2000, 10000}
    };
    
    for (const auto& [users, orders] : dataset_sizes) {
        table_manager->get_table("users")->clear();
        table_manager->get_table("orders")->clear();
        
        DataGenerator::generate_uniform_dataset(*table_manager, users, orders);
        
        SelectStatement join_stmt;
        join_stmt.from_table = TableReference("users");
        
        JoinClause join(JoinClause::INNER, TableReference("orders"), 
                       std::make_unique<BinaryOpExpression>(
                           std::make_unique<ColumnExpression>("users", "id"),
                           std::make_unique<ColumnExpression>("orders", "user_id"),
                           BinaryOperator::EQUALS
                       ));
        join_stmt.joins.push_back(std::move(join));
        
        SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
        join_stmt.select_list.push_back(std::move(all_item));
        
        auto best_plan = optimizer.optimize(join_stmt);
        double exec_time = measure_execution_time(*best_plan);
        auto result = executor.execute(*best_plan);
        
        results.emplace_back("Scalability_" + std::to_string(users) + "_" + std::to_string(orders), 
                           "Optimized", exec_time, best_plan->cost.total_cost, result->size());
        
        std::cout << "Dataset: " << users << " users, " << orders << " orders"
                  << " | Time: " << std::fixed << std::setprecision(2) << exec_time << "ms"
                  << " | Results: " << result->size() << std::endl;
    }
}

void QueryBenchmark::benchmark_data_distributions() {
    std::cout << "\n=== Data Distribution Benchmarks ===" << std::endl;
    
    std::vector<std::pair<std::string, std::function<void()>>> distributions = {
        {"Uniform", [this]() { DataGenerator::generate_uniform_dataset(*table_manager, 1000, 5000); }},
        {"Skewed", [this]() { DataGenerator::generate_skewed_dataset(*table_manager, 1000, 5000); }}
    };
    
    for (const auto& [dist_name, generator] : distributions) {
        table_manager->get_table("users")->clear();
        table_manager->get_table("orders")->clear();
        
        generator();
        
        SelectStatement join_stmt;
        join_stmt.from_table = TableReference("users");
        
        JoinClause join(JoinClause::INNER, TableReference("orders"), 
                       std::make_unique<BinaryOpExpression>(
                           std::make_unique<ColumnExpression>("users", "id"),
                           std::make_unique<ColumnExpression>("orders", "user_id"),
                           BinaryOperator::EQUALS
                       ));
        join_stmt.joins.push_back(std::move(join));
        
        SelectItem all_item(std::make_unique<ColumnExpression>("", "*"));
        join_stmt.select_list.push_back(std::move(all_item));
        
        auto best_plan = optimizer.optimize(join_stmt);
        double exec_time = measure_execution_time(*best_plan);
        auto result = executor.execute(*best_plan);
        
        results.emplace_back("Distribution_" + dist_name, "Optimized", 
                           exec_time, best_plan->cost.total_cost, result->size());
        
        std::cout << dist_name << " distribution"
                  << " | Time: " << std::fixed << std::setprecision(2) << exec_time << "ms"
                  << " | Results: " << result->size() << std::endl;
    }
}

void QueryBenchmark::print_results() const {
    std::cout << "\n=== Detailed Results ===" << std::endl;
    std::cout << std::left << std::setw(25) << "Query" 
              << std::setw(15) << "Plan Type"
              << std::setw(12) << "Time (ms)"
              << std::setw(15) << "Est. Cost"
              << std::setw(12) << "Result Size" << std::endl;
    std::cout << std::string(80, '-') << std::endl;
    
    for (const auto& result : results) {
        std::cout << std::left << std::setw(25) << result.query_name
                  << std::setw(15) << result.plan_type
                  << std::fixed << std::setprecision(2) << std::setw(12) << result.execution_time_ms
                  << std::setw(15) << result.estimated_cost
                  << std::setw(12) << result.result_size << std::endl;
    }
}

void QueryBenchmark::print_summary() const {
    std::cout << "\n=== Benchmark Summary ===" << std::endl;
    
    double total_time = 0;
    size_t join_count = 0;
    double join_time = 0;
    
    for (const auto& result : results) {
        total_time += result.execution_time_ms;
        if (result.query_name == "Join") {
            join_count++;
            join_time += result.execution_time_ms;
        }
    }
    
    std::cout << "Total queries executed: " << results.size() << std::endl;
    std::cout << "Total execution time: " << std::fixed << std::setprecision(2) << total_time << "ms" << std::endl;
    std::cout << "Average query time: " << std::fixed << std::setprecision(2) << (total_time / results.size()) << "ms" << std::endl;
    
    if (join_count > 0) {
        std::cout << "Average join time: " << std::fixed << std::setprecision(2) << (join_time / join_count) << "ms" << std::endl;
    }
}