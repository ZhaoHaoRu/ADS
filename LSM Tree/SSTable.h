#include <iostream>
#include <string>
#include <cstdint>
#include <stdint.h>
#include <utility>
#include <vector>
#include <memory>
#include <list>
#include <utility>
#include <fstream>
#include <limits.h>
#include <algorithm>
#include "BloomFilter.h"
#include "SkipList.h"

struct Index{
    uint64_t key;
    uint32_t offset;

    Index(uint64_t k = 0, uint32_t o = 0){
        key = k;
        offset = o;
    }
};


class SSTableCache{
public:
    uint64_t timeStamp;
    uint64_t KVNum;
    uint64_t minKey;
    uint64_t maxKey;
    BloomFilter *bloom;
    std::vector<Index> indexTable;
    //存储的路径，这里假设存储的路径即"./data/level-0"的形式
    std::string addr;

    //用目前已经存在的文件初始化SSTableCache
    SSTableCache();
    SSTableCache(std::string path, uint64_t &ts, std::string dir);
    SSTableCache(std::string &bufferStr, std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min = 0, uint64_t max = 0);
    bool Search(const uint64_t &key, uint32_t &offset, uint32_t &length, bool &isEnd);
    // std::vector<SSTableCache> Merge(std::vector<SSTableCache> &former); 
    std::string toFileName();
    void Reset();
    uint64_t getKVNum(){return KVNum; }
    std::string getAddr(){return addr; }
    ~SSTableCache();
};


class SSTable{
private:
    //存储的路径，这里假设存储的路径即"./data/level-0"的形式
    std::string addr;
    uint64_t timeStamp;
    uint64_t size;
    uint64_t count;
    // std::string valStr;
    std::list<std::pair<uint64_t, std::string>> table;

public:
    //默认的构造函数，用于写入文件时的初始化
    SSTable();
    SSTable(SSTableCache *cache);
    //在存储SSTable的文件中获取特定的string
    std::string ReadSSTable(uint32_t offset, uint32_t length, std::string fileName, bool isEnd);
    //把SSTable中的所有内容写入table
    void LoadSSTable(std::string &addr);
    std::string getAddr();
    void setAddr(std::string &address);
    //将SSTable中的内容写入内存
    void SSTableToFile(SSTableCache *cache);
    SSTableCache *SaveSSTable(std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min = 0, uint64_t max = 0);
    SSTable* merge(const SSTable *table1, const SSTable *table2);
    void mergeSort(std::vector<SSTable*> &Tlist);
    std::vector<SSTableCache*> division(SSTable *Table);
    ~SSTable();
};