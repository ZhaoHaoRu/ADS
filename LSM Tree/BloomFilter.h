#pragma once

#include <iostream>
#include <bitset>
#include <cstdint>
#include <stdint.h>

const unsigned int FilterSize = 81920;

class BloomFilter{
private:
    std::bitset<FilterSize> bits;

public:
    BloomFilter();
    BloomFilter(const char *buffer);
    char* writeTofile(char *buffer);
    void Insert(const uint64_t &key);
    bool Search(const uint64_t &key);
};