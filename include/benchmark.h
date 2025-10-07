#pragma once
#include "table.h"
#include "optimizer.h"
#include "executor.h"
#include <chrono>
#include <vector>
#include <string>

struct BenchmarkResult {
    std::string query_name;
    std::string plan_type;
    double execution_time_ms;
    double estimated_cost;
    size_t result_size;
    
    BenchmarkResult(const std::string& name, const std::string& type, 
                   double time, double cost, size_t size)
        : query_name(name), plan_type(type), execution_time_ms(time), 
          estimated_cost(cost), result_size(size) {}
};

class DataGenerator {
public:
    static void generate_large_dataset(TableManager& tm, size_t users_count, size_t orders_count);
    static void generate_skewed_dataset(TableManager& tm, size_t users_count, size_t orders_count);
    static void generate_uniform_dataset(TableManager& tm, size_t users_count, size_t orders_count);
};

class QueryBenchmark {
private:
    TableManager* table_manager;
    QueryOptimizer optimizer;
    Executor executor;
    std::vector<BenchmarkResult> results;
    
    double measure_execution_time(const PlanNode& plan);
    std::vector<SelectStatement> generate_test_queries();
    
public:
    QueryBenchmark(TableManager* tm);
    
    void run_single_table_benchmarks();
    void run_join_benchmarks();
    void run_selectivity_benchmarks();
    void run_scalability_benchmarks();
    
    void compare_join_algorithms(const SelectStatement& stmt);
    void benchmark_data_distributions();
    
    void print_results() const;
    void print_summary() const;
    void export_csv(const std::string& filename) const;
};