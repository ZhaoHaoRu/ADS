//
// Created by Lenovo on 2022/5/12.
//
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <stack>
#include <climits>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string.h>

using namespace std;
#define INF 1e8

class FixedSPParallelDijkstra
{
public:
    int N; // the number of elements
    int source;
    vector<vector<int>> distance;
    vector<vector<int>> graph; // the graph
    vector<vector<int>> paths; // the path from one node to other node
    void backtrack(vector<vector<int>>& res, vector<int>& output, int first, int len);
    int maxTreads; //the number of threads
    mutex mtx;
    FixedSPParallelDijkstra(vector<vector<int>> matrix);
    ~FixedSPParallelDijkstra() {}

    //calculate the min distance from one node
    void calLength(vector<int> &combination, vector<int> &lengths, int id);
    void parallel_dijkstra(int node, vector<int> &dist);
    vector<int> findPath(int node, vector<int> &searchPath);
    vector<vector<int>> permutation(vector<int> &intermediates);
};

