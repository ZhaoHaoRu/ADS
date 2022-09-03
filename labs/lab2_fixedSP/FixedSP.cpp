#include "FixedSP.h"

FixedSP::FixedSP(vector<vector<int>> matrix)
{
    N = matrix.size();
    graph = matrix;
    for(int i = 0; i < N; ++i){
        vector<int> tmp;
        paths.emplace_back(tmp);
    }
    distance = matrix;
}

void FixedSP::dijkstra(int node)
{
    vector<bool> visited(N, false);
    distance[node][node] = 0;
    visited[node] = true;
    vector<int> nowPath(N, -1);
    // nowPath.emplace_back(node);
    for(int i = 0; i < N; ++i){
        int v = -1, mindis = INF;
        for(int j = 0; j < N; ++j){
            if(!visited[j] && distance[node][j] < mindis){
                mindis = distance[node][j];
                v = j;
            }
        }
        if(v == -1)
            break;
        visited[v] = true;
        // nowPath.emplace_back(v);
        for(int j = 0; j < N; ++j){
            if(!visited[j] && (distance[node][j] > distance[node][v] + graph[v][j])) {
                distance[node][j] = distance[node][v] + graph[v][j];
                nowPath[j] = v;
            }
        }
    }
    paths[node] = nowPath;
}

vector<int> FixedSP::findPath(int node, vector<int> &searchPath)
{
    vector<int> result;
    stack<int> tmp;
    int pre = searchPath[node];
    while(pre != -1){
        tmp.push(pre);
        pre = searchPath[pre];
    }
    while(!tmp.empty()){
        int top = tmp.top();
        tmp.pop();
        result.emplace_back(top);
    }
    result.emplace_back(node);
    return result;
}

void FixedSP::backtrack(vector<vector<int>>& res, vector<int>& output, int first, int len) 
{
    if(first == len){
        res.emplace_back(output);
        return;
    }
    for(int i = first; i < len; ++i) {
        swap(output[i], output[first]);
        backtrack(res, output, first + 1, len);
        swap(output[i], output[first]);
    }
}

vector<vector<int>> FixedSP::permutation(vector<int> &intermediates) 
{
    vector<vector<int>> res;
    backtrack(res, intermediates, 0, (int)intermediates.size());
        return res;
}

vector<int> FixedSP::getFixedPointShortestPath(int source, vector<int> intermediates)
{
    vector<int> path;

    vector<vector<int>> permutations = permutation(intermediates);

    //important: initalize!!!
    distance = graph;

    //find min path between two nodes
    dijkstra(source);
    int len = intermediates.size();

    for(int i = 0; i < len; ++i) {
        dijkstra(intermediates[i]);
    }

    //find min path and its combination
    int minPath = INF;
    vector<int> minCombination;
    int size = permutations.size();
    for(int i = 0; i < size; ++i) {
        vector<int> combination = permutations[i];
        long long length = distance[source][combination[0]] + distance[combination[len - 1]][source];
        for(int j = 1; j < len; ++j){
            length += distance[combination[j - 1]][combination[j]];
        }
        if(length < minPath) {
            minPath = length;
            minCombination = combination;
        }
    }

    //build the path
    if(minPath == INF)
        return path;
    minCombination.insert(minCombination.begin(), source);
    minCombination.emplace_back(source);
    len = minCombination.size();
    path.emplace_back(source);
    for(int i = 1; i < len; ++i){
        int node = minCombination[i];
        vector<int> nowPath = paths[minCombination[i - 1]];
        vector<int> partPath = findPath(node, nowPath);
        path.insert(path.end(), partPath.begin(), partPath.end());
    }
    return path;
}
