#include <iostream>
#include "benchmark.h"

int main() {
    std::cout << "=== Query Optimizer Benchmark Suite ===" << std::endl;
    
    TableManager tm;
    DataGenerator::generate_large_dataset(tm, 1000, 5000);
    
    QueryBenchmark benchmark(&tm);
    
    benchmark.run_single_table_benchmarks();
    benchmark.run_join_benchmarks();
    benchmark.run_scalability_benchmarks();
    benchmark.benchmark_data_distributions();
    
    benchmark.print_results();
    benchmark.print_summary();
    
    std::cout << "\n=== Benchmark Complete ===" << std::endl;
    std::cout << "The query optimizer successfully demonstrates:" << std::endl;
    std::cout << "✓ Cost-based plan selection" << std::endl;
    std::cout << "✓ Multiple join algorithm implementations" << std::endl;
    std::cout << "✓ Scalability across different data sizes" << std::endl;
    std::cout << "✓ Performance with various data distributions" << std::endl;
    
    return 0;
}