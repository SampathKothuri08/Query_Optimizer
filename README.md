# Query Optimizer

A C++ implementation of a database query optimizer that makes SQL queries run faster by automatically choosing the best execution strategy.

## What it does

When you run a SQL query like `SELECT * FROM users JOIN orders ON users.id = orders.user_id`, this optimizer:

1. **Parses** the SQL into something the computer understands
2. **Generates** multiple ways to execute the query  
3. **Estimates** how long each approach will take
4. **Picks** the fastest one automatically

**Result**: Queries that took 7+ seconds now run in 11 milliseconds (99.8% faster!)

## How to run it

```bash
# Compile and run the demo
g++ -std=c++17 -I include demo.cpp src/tokenizer.cpp src/optimizer.cpp src/cost_model.cpp src/plan_builder.cpp src/executor.cpp src/benchmark.cpp -o demo
./demo
```

## The three join algorithms

When combining data from two tables, there are different strategies:

- **Nested Loop**: Check every row against every other row (slow but simple)
- **Hash Join**: Build a lookup table first, then match (fast for most cases)  
- **Sort-Merge**: Sort both tables, then merge them (efficient for large sorted data)

Our optimizer automatically picks the best one based on data size and patterns.

## Key components

- `tokenizer.cpp` - Breaks SQL into pieces
- `optimizer.cpp` - Chooses the best execution plan
- `executor.cpp` - Actually runs the queries
- `benchmark.cpp` - Tests performance

## Performance results

Testing with 1,000 users and 5,000 orders:

| Algorithm | Time | 
|-----------|------|
| Nested Loop | 7,144ms |
| Hash Join | 11ms  |
| Sort-Merge | 15ms |

The optimizer correctly picked Hash Join as the fastest option.


Real databases like PostgreSQL and MySQL use similar optimizers. Understanding how they work helps you:
- Write better SQL queries
- Debug performance problems  
- Understand database internals
