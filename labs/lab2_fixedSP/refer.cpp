/*
时间复杂度: O(n^2)
输入: 
n 表示图中的点数
g g[i][j]表示i到j之间边的距离
输出:
dist dist[i]表示node到i的最短距离
*/
const int N=1005;
int dist[N];
int g[N][N];
bool vis[N];
void dijkstra(int n, int node){
    memset(vis, 0x00, sizeof vis);
    memset(dist, 0x3f, sizeof dist);
    dist(node)=0;

    for(int i=1; i<=n; ++i){ // loop n rounds
        int v=-1, mindis=0x3f3f3f3f;
        for(int j=1; j<=n; ++j) // 找到未访问的最小距离
            if(!vis[j] && dist[j]<mindis){
                mindis=dist[j];
                v=j;
            }
        if(v==-1) break;
        vis[v]=true;
        for(int j=1; j<=n; ++j){
            if(!vis[j]) dist[j]=min(dist[j], dist[v]+g[v][j]);
        }
    }
}

void backtrack(vector<vector<int>>& res, vector<int>& output, int first, int len){
        // 所有数都填完了
        if (first == len) {
            res.emplace_back(output);
            return;
        }
        for (int i = first; i < len; ++i) {
            // 动态维护数组
            swap(output[i], output[first]);
            // 继续递归填下一个数
            backtrack(res, output, first + 1, len);
            // 撤销操作
            swap(output[i], output[first]);
        }
    }
    vector<vector<int>> permute(vector<int>& nums) {
        vector<vector<int> > res;
        backtrack(res, nums, 0, (int)nums.size());
        return res;
    }

