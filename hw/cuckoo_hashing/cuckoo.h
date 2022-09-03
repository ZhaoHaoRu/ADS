//
// Created by Haoru Zhao on 2022/5/21.
//

#ifndef CUCKOO_HASHING_CUCKOO_H
#define CUCKOO_HASHING_CUCKOO_H

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <vector>
#include <thread>
#include <mutex>
#define SIZE (10000)

typedef int KeyType;
const int bound = 1000;
class Cuckoo{
protected:
    std::mutex mtx;
    KeyType T[SIZE];
    // hash key by hash func 1
    int hash1(const KeyType &key);
    // hash key by hash func 2
    int hash2(const KeyType &key);
    // find key by hash func 1 in T, exist return key otherwise 0
    KeyType get1(const KeyType &key);
    // find key by hash func 2 in T, exist return key otherwise 0
    KeyType get2(const KeyType &key);
    void bt_evict(const KeyType &key, int which, int pre_pos);
//    void rehash();
public:
    Cuckoo();
    ~Cuckoo();
    KeyType get(const KeyType &key);
    void put(const KeyType &key);
};

#endif //CUCKOO_HASHING_CUCKOO_H
