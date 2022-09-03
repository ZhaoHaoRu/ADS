#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <stack>
#include <string.h>

using namespace std;

#define INF 1e8

class FixedSP
{
private:
    int N; // the number of elements
    vector<vector<int>> distance;
    vector<vector<int>> graph; // the graph
    vector<vector<int>> paths; // the path from one node to other node
    void backtrack(vector<vector<int>>& res, vector<int>& output, int first, int len);
public:
    FixedSP(vector<vector<int>> matrix);
    ~FixedSP() {}

    //calculate the min distance from one node
    void dijkstra(int node);
    vector<int> findPath(int node, vector<int> &searchPath);
    vector<vector<int>> permutation(vector<int> &intermediates);
    vector<int> getFixedPointShortestPath(int source, vector<int> intermediates);
};
