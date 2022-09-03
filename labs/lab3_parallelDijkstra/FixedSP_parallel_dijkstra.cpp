#include "FixedSP_parallel_dijkstra.h"


FixedSPParallelDijkstra::FixedSPParallelDijkstra(vector<vector<int>> matrix) {
    N = matrix.size();
    graph = matrix;
    for(int i = 0; i < N; ++i){
        vector<int> tmp;
        paths.emplace_back(tmp);
    }
    distance = matrix;
    maxTreads = 8;
}


void FixedSPParallelDijkstra::parallel_dijkstra(int node, vector<int> &dist)
{
    vector<bool> visited(N, false);
    dist[node] = 0; // dist is a local variable in fact
    visited[node] = true;
    vector<int> nowPath(N, -1);
    for(int i = 0; i < N; ++i){
        int v = -1, mindis = INF;
        for(int j = 0; j < N; ++j){
            if(!visited[j] && dist[j] < mindis){
                mindis = dist[j];
                v = j;
            }
        }
        if(v == -1)
            break;
        visited[v] = true;
        for(int j = 0; j < N; ++j){
            // Here, `graph` which shared by threads is read-only, so there will be no race and other problems
            if(!visited[j] && (dist[j] > dist[v] + graph[v][j])) {
                dist[j] = dist[v] + graph[v][j];
                nowPath[j] = v;
            }
        }
    }

    // before write to `paths` and `distances` which shared by threads, lock to prepare first
//    lock_guard<mutex> lock(mtx);
    paths[node] = nowPath;
    distance[node] = dist;
}

void FixedSPParallelDijkstra::calLength(vector<int> &combination, vector<int> &lengths, int id) {
    int len = combination.size();
    // `distance` which shared by threads is read-only, so there will be no race and other problems
    long long length = distance[source][combination[0]] + distance[combination[len - 1]][source];
    for(int j = 1; j < len; ++j){
        length += distance[combination[j - 1]][combination[j]];
    }

    // before write to `lengths` and `distances` which shared by threads, lock to prepare first
//    lock_guard<mutex> lock(mtx);
    lengths[id] = length;
}

vector<int> FixedSPParallelDijkstra::findPath(int node, vector<int> &searchPath)
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

void FixedSPParallelDijkstra::backtrack(vector<vector<int>>& res, vector<int>& output, int first, int len)
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

vector<vector<int>> FixedSPParallelDijkstra::permutation(vector<int> &intermediates)
{
    vector<vector<int>> res;
    backtrack(res, intermediates, 0, (int)intermediates.size());
    return res;
}


