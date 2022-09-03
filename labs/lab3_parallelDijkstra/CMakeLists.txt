cmake_minimum_required(VERSION 3.21)
project(lab3)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lab3
        FixedSP.cpp
        FixedSP.h
        FixedSP_parallel_dijkstra.cpp
        FixedSP_parallel_dijkstra.h
        main.cpp)
