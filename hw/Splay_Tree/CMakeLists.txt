cmake_minimum_required(VERSION 3.21)
project(Splay_Tree)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(Splay_Tree
        main.cpp
        SplayTree.h)
