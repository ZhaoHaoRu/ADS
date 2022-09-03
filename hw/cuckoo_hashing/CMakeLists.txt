cmake_minimum_required(VERSION 3.21)
project(cuckoo_hashing)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(cuckoo_hashing
        cuckoo.cpp
        cuckoo.h
        main.cpp)
