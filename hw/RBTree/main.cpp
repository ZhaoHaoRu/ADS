#include <iostream>
#include <fstream>
#include <windows.h>
#include "RBTree.h"

using namespace std;

const int n = 100;

int main() {
    RBTree *tree = new RBTree();
    ifstream inputData;
    inputData.open("../input_2000_range.txt", ios::in);
    if(!inputData) {
        cout << "[error]: file " << " not found." << endl;
        inputData.close();
        exit(0);
    }
    string line;
    LARGE_INTEGER t1,t2,tc;
    while (inputData >> line)
    {
        int bracketPos = line.find('(');
        string op = line.substr(0, bracketPos);
        string param = line.substr(bracketPos + 1, line.size() - bracketPos - 2);
        if (op == "Insert")
        {
            int commaPos = param.find(',');
            int key = atoi(param.substr(0, commaPos).c_str());
            int val = atoi(param.substr(commaPos + 1).c_str());
            tree->insert(key, val);
        }
    }
    inputData.close();
    inputData.open("../search_2000_order.txt", ios::in);
    if(!inputData) {
        cout << "[error]: file " << " not found." << endl;
        inputData.close();
        exit(0);
    }
    RBNode *prev;
    QueryPerformanceFrequency(&tc);
    QueryPerformanceCounter(&t1);
    while (inputData >> line)
    {
        prev = nullptr;
        int bracketPos = line.find('(');
        string op = line.substr(0, bracketPos);
        string param = line.substr(bracketPos + 1, line.size() - bracketPos - 2);
        int commaPos = param.find(',');
        int key = atoi(param.substr(0, commaPos).c_str());
        tree->search(key, &prev);
    }
    QueryPerformanceCounter(&t2);
    inputData.close();
    double time = (double)(t2.QuadPart-t1.QuadPart);
    cout<<"time = "<<time<<endl;  //输出时间（单位：ｓ）
    cout<< tree->rotateCount << endl;
    return 0;
//    int key = 0;
//    QueryPerformanceFrequency(&tc);
//    QueryPerformanceCounter(&t1);
//    RBNode *prev;
//    for(int i = 0;i < m; ++i){
//        prev = nullptr;
//        key = i % k + 1;
//        tree->search(key, &prev);
//    }
//
//    double time = (double)(t2.QuadPart-t1.QuadPart);
//    cout<<"time = "<<time<<endl;  //输出时间（单位：ｓ）
//    return 0;
}
