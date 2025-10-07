# Query Optimizer with Cost-Based Selection

A complete implementation of a rule-based and cost-based query optimizer in C++, featuring SQL parsing, multiple join algorithms, cost estimation, and performance benchmarking.

## 🎯 Project Overview

This project implements a fully functional database query optimizer that can:
- Parse SQL SELECT statements into Abstract Syntax Trees
- Generate multiple execution plans with different join algorithms
- Estimate costs using I/O and CPU models
- Select the optimal plan based on cost analysis
- Execute queries and return results
- Benchmark performance across different scenarios

## 🏗️ Architecture

### Core Components

1. **SQL Parser** (`tokenizer.h`, `parser.h`, `ast.h`)
   - Tokenizes SQL into meaningful tokens
   - Builds Abstract Syntax Trees from tokens
   - Supports SELECT, FROM, WHERE, JOIN clauses

2. **Query Plan Representation** (`query_plan.h`)
   - Tree-based execution plan structure
   - Nodes for TableScan, Filter, Project, Join operations
   - Built-in cost estimation for each operation

3. **Join Algorithms** (`executor.h`)
   - **Nested Loop Join**: O(n*m) simple algorithm
   - **Hash Join**: Efficient for equi-joins
   - **Sort-Merge Join**: Optimal for sorted data

4. **Cost Model** (`cost_model.h`)
   - I/O cost estimation (sequential vs random access)
   - CPU cost modeling for operations
   - Selectivity estimation for filters
   - Join cardinality estimation

5. **Query Optimizer** (`optimizer.h`)
   - Plan enumeration with different join orders
   - Cost-based plan selection
   - Optimization report generation

6. **Execution Engine** (`executor.h`, `table.h`)
   - In-memory table management
   - Query execution with result sets
   - Real data processing

7. **Benchmarking Framework** (`benchmark.h`)
   - Performance testing across data sizes
   - Join algorithm comparison
   - Data distribution analysis

## 🚀 Getting Started

### Prerequisites
- C++17 compatible compiler (g++, clang++)
- CMake (optional, manual compilation works)

### Building the Project

```bash
# Manual compilation
g++ -std=c++17 -I include demo.cpp src/*.cpp -o demo

# Or using individual test files
g++ -std=c++17 -I include test_optimizer.cpp src/optimizer.cpp src/cost_model.cpp src/plan_builder.cpp src/executor.cpp -o test_optimizer
```

### Running Tests

```bash
# Complete demonstration
./demo

# Individual component tests
./test_optimizer    # Cost-based optimization
./test_executor     # Query execution
./test_plan         # Plan generation
./test_benchmark    # Performance benchmarking
```

## 📊 Performance Results

The optimizer demonstrates significant improvements:

### Join Algorithm Comparison
- **Nested Loop Join**: ~413,245 cost units
- **Hash Join**: ~4,255 cost units  
- **Sort-Merge Join**: ~1,117 cost units (SELECTED)

**Result**: 99.73% performance improvement through intelligent algorithm selection!

### Query Examples

```sql
-- Single table query with filter
SELECT name, age FROM users WHERE age > 25;

-- Join query (automatically optimized)
SELECT * FROM users JOIN orders ON users.id = orders.user_id;
```

## 🎯 Key Features Implemented

✅ **SQL Parsing**: Complete tokenizer and parser for SELECT statements  
✅ **AST Generation**: Abstract syntax trees for query representation  
✅ **Plan Trees**: Hierarchical execution plan structure  
✅ **Join Algorithms**: Three different join implementations  
✅ **Cost Estimation**: Realistic I/O and CPU cost models  
✅ **Plan Optimization**: Cost-based plan selection  
✅ **Query Execution**: Real data processing and results  
✅ **Benchmarking**: Performance evaluation framework  

## 📁 Project Structure

```
Query_Optimizer/
├── include/
│   ├── tokenizer.h      # SQL tokenization
│   ├── parser.h         # SQL parsing
│   ├── ast.h           # Abstract syntax trees
│   ├── query_plan.h    # Execution plans
│   ├── plan_builder.h  # Plan construction
│   ├── cost_model.h    # Cost estimation
│   ├── optimizer.h     # Query optimization
│   ├── executor.h      # Query execution
│   ├── table.h         # Table management
│   └── benchmark.h     # Performance testing
├── src/
│   ├── tokenizer.cpp
│   ├── parser.cpp
│   ├── plan_builder.cpp
│   ├── cost_model.cpp
│   ├── optimizer.cpp
│   ├── executor.cpp
│   └── benchmark.cpp
├── test_*.cpp          # Individual tests
├── demo.cpp           # Complete demonstration
└── README.md
```

## 🧪 Testing & Validation

The project includes comprehensive testing:

1. **Unit Tests**: Individual component validation
2. **Integration Tests**: End-to-end query processing
3. **Performance Tests**: Scalability across data sizes
4. **Benchmark Suite**: Join algorithm comparison

### Sample Output

```
=== Query Optimization Report ===
Generated 6 plan alternatives:

Plan 1: NestedLoopJoin - Cost: 413,245
Plan 2: HashJoin - Cost: 4,255  
Plan 3: SortMergeJoin - Cost: 1,117 ⭐ SELECTED

*** 99.73% performance improvement achieved! ***
```

## 🔍 Technical Details

### Cost Model
- **I/O Costs**: Sequential (1.0) vs Random (4.0) access
- **CPU Costs**: Tuple processing (0.01) and operations (0.0025)
- **Join Costs**: Algorithm-specific estimation
- **Selectivity**: Filter and join cardinality estimation

### Join Algorithm Selection
- **Small tables**: Hash Join preferred
- **Large sorted tables**: Sort-Merge Join optimal
- **Cross products**: Nested Loop as fallback

### Optimization Strategies
- Plan enumeration with multiple join orders
- Cost-based algorithm selection
- Filter pushdown optimization
- Cardinality-driven decisions

## 🎓 Educational Value

This project demonstrates:
- **Database Internals**: Query processing pipeline
- **Algorithm Analysis**: Join algorithm trade-offs
- **Cost Modeling**: Performance prediction
- **System Design**: Modular architecture
- **C++ Programming**: Modern C++17 features

## 🤝 Contributing

This is an educational project showcasing database query optimization concepts. The implementation focuses on clarity and educational value rather than production optimization.

## 📝 License

Educational project - feel free to study, modify, and learn from the code!

---

**Built with ❤️ for learning database systems and query optimization**