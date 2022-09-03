//
// Created by Lenovo on 2022/5/26.
//

void MakeSet(vector<int>& uset, int n)
{
    uset.assign(n,0);
    for (int i = 0; i < n; i++)
    uset[i] = i;
}

//查找当前元素所在集合的代表元
int FindSet(vector<int>& uset,int u)
{
    int i = u;
    while (uset[i] != i) i = uset[i];
    return i;
}

void Kruskal(const vector<Edge>& edges,int n)
{
    vector<int> uset;
    vector<Edge> SpanTree;
    int Cost = 0,e1,e2;
    MakeSet(uset,n);
    for (int i = 0; i < edges.size(); i++) {
        e1 = FindSet(uset,edges[i].u);
        e2 = FindSet(uset,edges[i].v);
        if (e1 != e2) {
        SpanTree.push_back(edges[i]);
        Cost += edges[i].w;
        uset[e1] = e2;
        }
    }
    cout << "Result:\n";
    cout << "Cost: " << Cost << endl;
    cout << "Edges:\n";
    for (int j = 0; j < SpanTree.size(); j++)
    cout << SpanTree[j].u << " " << SpanTree[j].v << " " << SpanTree[j].w << endl;
    cout << endl;
}

