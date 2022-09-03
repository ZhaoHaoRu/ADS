#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <time.h>
#include <windows.h>
#include "FixedSP.h"
#include "FixedSP_parallel_dijkstra.h"

using namespace std;

mutex mtx;

void threadPrepare(int id, vector<int> &arr, FixedSPParallelDijkstra &fsp)
{
    int perThread = arr.size() / fsp.maxTreads; // Loop processed by each thread
    int start = id * perThread;
    int end = start + perThread;
    int len = arr.size();
    if(id == fsp.maxTreads - 1) // If it is the last thread, all the remaining loops should be processed
        end = len;
    for(int i = start; i < end; ++i) {
        if(i >= len)
            break;
        vector<int> tmp = fsp.distance[arr[i]]; // copy fsp.distance[arr[i]]
        fsp.parallel_dijkstra(arr[i], tmp);
    }
}


void nextThreadPrepare(int id, vector<vector<int>> &permutations, vector<int> &length, FixedSPParallelDijkstra &fsp) {
    int perThread = permutations.size() / fsp.maxTreads;
    int start = id * perThread;
    int end = start + perThread;
    int len = permutations.size();
    if(id == fsp.maxTreads - 1)
        end = len;
    for(int i = start; i < end; ++i) {
        if(i >= len)
            break;
        vector<int> combination = permutations[i];
        fsp.calLength(combination, length, i);
    }
}
vector<int> getFixedPointShortestPath(int source, vector<int> intermediates, FixedSPParallelDijkstra &fsp)
{
    vector<int> path;
    fsp.source = source;
    vector<vector<int>> permutations = fsp.permutation(intermediates);
    fsp.distance = fsp.graph;
    int len = intermediates.size();
    intermediates.emplace_back(source);
    vector<thread> threads;

    // create threads
    for(int i = 0; i < fsp.maxTreads ; ++i) {
        threads.emplace_back(threadPrepare, i, ref(intermediates), ref(fsp));
    }
    // Wait for all threads to finish before proceeding to the next stage
    for(int i = 0; i < fsp.maxTreads; ++i) {
        threads[i].join();
    }

    intermediates.pop_back();
    int minPath = INF;
    vector<int> minCombination;
    int size = permutations.size();
    vector<int> lengths(size, 0);
    vector<thread> nextThreads;
    for(int i = 0; i < fsp.maxTreads; ++i) {
        nextThreads.emplace_back(nextThreadPrepare, i, ref(permutations), ref(lengths), ref(fsp));
    }
    for(int i = 0; i < fsp.maxTreads; ++i) {
        nextThreads[i].join();
    }
    for(int i = 0; i < size; ++i) {
        if(lengths[i] < minPath) {
            minPath = lengths[i];
            minCombination = permutations[i];
        }
    }

    if(minPath == INF)
        return path;
    minCombination.insert(minCombination.begin(), source);
    minCombination.emplace_back(source);

    len = minCombination.size();
    path.emplace_back(source);
    for(int i = 1; i < len; ++i){
        int node = minCombination[i];
        vector<int> nowPath = fsp.paths[minCombination[i - 1]];
        vector<int> partPath = fsp.findPath(node, nowPath);
        path.insert(path.end(), partPath.begin(), partPath.end());
    }
    return path;
}


int main() {
    ifstream inputData;
    string input_file_path = "../input/1000_input.txt";
    inputData.open(input_file_path, ios::in);
    if (!inputData)
    {
        cout << "[error]: file " << input_file_path << " not found." << endl;
        inputData.close();
        return -1;
    }

    string str;
    inputData >> str;
    int node_num = atoi(str.c_str());

    vector<vector<int>> matrix(node_num, vector<int>(node_num));

    for(int i = 0; i < node_num; ++i){
        for(int j = 0; j < node_num; ++j){
            inputData >> str;
            matrix[i][j] = str == "@" ? INF : atoi(str.c_str());
        }
    }

    FixedSPParallelDijkstra fsp(matrix);

    while (inputData >> str)
    {
        inputData >> str;
        int source = atoi(str.c_str());
        vector<int> intermediates;
        while(true){
            inputData >> str;
            if(str == "$"){
                break;
            }
            intermediates.emplace_back(atoi(str.c_str()));
        }
        LARGE_INTEGER t1,t2,tc;
        QueryPerformanceFrequency(&tc);
        QueryPerformanceCounter(&t1);
        vector<int> path = getFixedPointShortestPath(source, intermediates, fsp);
        QueryPerformanceCounter(&t2);
        std::cout << "time: " << (double)(t2.QuadPart-t1.QuadPart) * 1000 / tc.QuadPart << endl;  //单位为ms
        int dis = 0;
        for(int i = 0; i < path.size(); ++i){
            dis += matrix[path[i]][path[(i + 1) % path.size()]];
        }
        if(dis == 0){
            dis = INF;
        }
        cout << dis << endl;
    }
    inputData.close();
    return 0;
}

