#include <iostream>
#include <cstdint>
#include <string>
#include <fstream>
#include <time.h>
#include <stdio.h>
#include "test.h"
#include "kvstore.h"


const int testSize = 10000;
int main(){
    std::cout << "begin test!" << std::endl;
    std::ofstream file("regular_test_20000.csv", std::ios::out);
    class KVStore store("./data_20000");
    store.reset();
    clock_t t1 = clock();
    //test put
    for(int i = 0; i < testSize; ++i){
        store.put(i, std::string(20000, 's'));
    }
    clock_t t2 = clock();
    std::cout << "put time: " << t2 - t1 << std::endl;
    file << "put time: " << "," << t2 - t1 << std::endl;
    double sec = (double)(t2 - t1) / 1000000;
    double Throughput = (double)testSize / (t2 - t1);
    double perTime = (double)(t2 - t1) / testSize;
    std::cout << "put: " << sec << " " << Throughput << " " << perTime << std::endl;
    file << "put: " << sec << "," << Throughput << "," << perTime << std::endl;

    //test get
    t1 = clock();
    for(int i = 0; i < testSize; ++i){
        store.get(i);
    }
    t2 = clock();
    std::cout << "get time: " << t2 - t1 << std::endl;
    file << "get time: " << "," <<t2 - t1 << std::endl;
    sec = (double)(t2 - t1) / 1000000;
    Throughput = (double)testSize / (t2 - t1);
    perTime = (double)(t2 - t1) / testSize;
    std::cout << "get: " <<sec << " " << Throughput << " " << perTime << std::endl;
    file << "get: " <<sec << "," << Throughput << "," << perTime << std::endl;

    //test delete
    t1 = clock();
    for(int i = 0; i < testSize; ++i){
        store.get(i);
    }
    t2 = clock();
    std::cout << "delete time: " << t2 - t1 << std::endl;
    file << "delete time: " << "," <<t2 - t1 << std::endl;
    sec = (double)(t2 - t1) / 1000000;
    Throughput = (double)testSize / (t2 - t1);
    perTime = (double)(t2 - t1) / testSize;
    std::cout << "delete: " <<sec << " " << Throughput << " " << perTime << std::endl;
    file << "delete: " <<sec << "," << Throughput << "," << perTime << std::endl;
    file.close();
}