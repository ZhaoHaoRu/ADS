//
// Created by Haoru Zhao on 2022/5/21.
//

#include <iostream>
#include <windows.h>
#include <time.h>
#include "cuckoo.h"
#include <vector>

using namespace std;

const double alpha = 0.1;   // load factor
int main(){
    int count = SIZE * alpha;
    Cuckoo cuckoo;
    for(int i = 1; i <= count; ++i) {
        if(i == 5001){
            ++count;
            continue;
        }
        cuckoo.put(i);
    }
    LARGE_INTEGER t1,t2,tc;
    QueryPerformanceFrequency(&tc);
    QueryPerformanceCounter(&t1);
    for(int i = 1; i <= count; ++i) {
        cuckoo.get(i);
    }
    QueryPerformanceCounter(&t2);
    std::cout << "insert average time: " << (double)(t2.QuadPart-t1.QuadPart) / count << endl;
    return 0;
}