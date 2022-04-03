#pragma once

#include <vector>
#include <climits>
#include <time.h>
#include <list>
#include <limits.h>
#include <iostream>
#include <utility>

#define MAX_LEVEL 18

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    std::string val;
    SKNodeType type;
    std::vector<SKNode *> forwards;
    SKNode(uint64_t _key, std::string _val, SKNodeType _type)
        : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            forwards.push_back(nullptr);
        }
    }
};

class SkipList
{
private:
    SKNode *head;
    SKNode *NIL;
    // unsigned long long s = 1;
    signed long long remainSpace;
    double my_rand();
    int randomLevel();
    uint64_t listSize;

public:
    SkipList();
    bool Insert(uint64_t key, std::string value);
    bool Search(uint64_t key, std::string &value, bool &hasDel);
    bool Delete(uint64_t key);
    std::list<std::pair<uint64_t, std::string>> Scan(uint64_t key_start, uint64_t key_end);
    void Display();
    void WriteOut(uint64_t &min, uint64_t &max, uint32_t &size, std::vector<std::pair<uint64_t, std::string>> &li);
    void Clear();
    ~SkipList()
    {
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
    }
};
