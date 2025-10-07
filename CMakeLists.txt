cmake_minimum_required(VERSION 3.15)
project(QueryOptimizer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

add_executable(query_optimizer 
    src/main.cpp
    src/tokenizer.cpp
)

enable_testing()
add_subdirectory(tests)