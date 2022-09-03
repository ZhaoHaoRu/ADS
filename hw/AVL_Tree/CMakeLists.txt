cmake_minimum_required(VERSION 3.21)
project(AVL_Tree)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(AVL_Tree
        AVLTree.cpp
        AVLTree.h
        main.cpp)
