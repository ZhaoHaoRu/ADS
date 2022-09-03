#include <iostream>
#include <windows.h>
#include <fstream>
using namespace std;
#include "SplayTree.h"

using namespace std;
const int n = 10000;
const int m = 10000;
const int k = 25;
int main()
{

    SplayTree<int, int> a;  //both key and value are of int type. They can be of any type. They
    // both can be of different type. If key is of nonprimitive type, then <, >, == operators must be overloaded.
    // To ensure that value is always deep copied, assignment operator (=) must be overloaded.
    LARGE_INTEGER t1,t2,tc;
    QueryPerformanceFrequency(&tc);
    QueryPerformanceCounter(&t1);
    for (int i = 0; i < n; ++i) {
        a.insert(i + 1, i + 1);
    }
    QueryPerformanceCounter(&t2);
    std::cout << "insert average time: " << (double)(t2.QuadPart-t1.QuadPart) / n<< endl;

    QueryPerformanceCounter(&t1);
    for(int i = 0; i < n; ++i){
        a.search(i);
    }
    QueryPerformanceCounter(&t2);
    std::cout << "search average time: " << (double)(t2.QuadPart-t1.QuadPart) / n << endl;

    QueryPerformanceCounter(&t1);
    for(int i = n; i > 1; --i){
//        cout << i << endl;
        a.deleteKey(i);
    }
    QueryPerformanceCounter(&t2);
    std::cout << "delete average time: " << (double)(t2.QuadPart-t1.QuadPart) / n << endl;

//    ifstream inputData;
//    inputData.open("../skiplist_create_input100", ios::in);
//    LARGE_INTEGER t1,t2,tc;
//    if(!inputData) {
//        cout << "[error]: file " << " not found." << endl;
//        inputData.close();
//        exit(0);
//    }
//    string line;
//    while (inputData >> line)
//    {
//        int bracketPos = line.find('(');
//        string op = line.substr(0, bracketPos);
//        string param = line.substr(bracketPos + 1, line.size() - bracketPos - 2);
//        if (op == "Insert")
//        {
//            int commaPos = param.find(',');
//            int key = atoi(param.substr(0, commaPos).c_str());
//            int val = atoi(param.substr(commaPos + 1).c_str());
//            a.insert(key, val);
//        }
//
//    }
//    inputData.close();
////    for (int i = 1; i <= 400; i++)
////        a.insert(i, i);
//    int key = 0;
//    QueryPerformanceFrequency(&tc);
//    QueryPerformanceCounter(&t1);
//    for(int i = 0;i < m; ++i){
//        key = i % k + 1;
//        a.search(key);
//    }
//    QueryPerformanceCounter(&t2);
//    double time = (double)(t2.QuadPart-t1.QuadPart);
//    cout<<"time = "<<time<<endl;  //输出时间（单位：ｓ）
    return 0;

}//
// Created by Lenovo on 2022/3/25.
//

