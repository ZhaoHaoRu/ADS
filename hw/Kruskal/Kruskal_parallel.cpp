//
// Created by Lenovo on 2022/5/26.
//

#include<iostream>
#include<vector>
#include<thread>
#include<map>
#include<fstream>
#include <string>
#include<mutex>
#include<windows.h>
#include<condition_variable>
using namespace std;
class edge {
public:
    edge() {}
    edge(int a,int b,int c):v1(a),v2(b),w(c),next(NULL) {}
    int v1,v2;
    int w,tid;
    edge* next;
};

class vertex {
public:
    vertex(int v):vid(v),next(NULL) {}
    int vid;
    edge* next;
};

const int thread_num = 3;//线程数量
vector<vertex> graph;//图邻接表
vector<edge> mst;//结果集
vector<thread> subthreads;//线程向量
multimap<int,edge> edge_queue;//全局队列
mutex mut;//互斥锁
condition_variable cond_v[thread_num];//条件变量
bool isfinished = false;



void construct()
{
    string filename = "../10_20_input.txt";
    ifstream file(filename);
    string str;
    file >> str;
    int v_num = atoi(str.c_str());
    file >> str;
    int a_num = atoi(str.c_str());

    for (int i = 1; i <= v_num; i++)
        graph.push_back(vertex(i));
    for (int i = 0; i < a_num; i++){
        edge *temp1 = new edge(),*temp2;
        file >> str;
        temp1->v1 = atoi(str.c_str());
//        cout << temp1->v1 << " ";
        file >> str;
        temp1->v2 = atoi(str.c_str());
//        cout << temp1->v2 << " ";
        file >> str;
        temp1->w = atoi(str.c_str());
//        cout << temp1->w << " " << endl;
        temp2 = new edge(*temp1);
        temp1->next = graph[temp1->v1 - 1].next;
        graph[temp1->v1 - 1].next = temp1;
        temp2->next = graph[temp2->v2 - 1].next;
        graph[temp2->v2 - 1].next = temp2;
    }
}

void send_edge(multimap<int,edge>& m) {
    if (!m.empty()) {
        edge_queue.insert(*(m.begin()));
        m.erase(m.begin());
    }
}

void subthread_func(vector<vertex> v,int tid)
{
    multimap<int,edge> e;
    // 将该部分图中的所有结点的相关边加入map
    for (int i = 0; i < v.size(); i++){
        edge* temp = v[i].next;
        while (temp != NULL){
            temp->tid = tid;
            e.insert(pair<int,edge>(temp->w,*temp));
            temp = temp->next;
        }
    }

    unique_lock<mutex> lk(mut);
    send_edge(e);

    while (true){
        cond_v[tid].wait(lk);
        //所有的顶点都找到了
        if (isfinished)
            return;
        send_edge(e);
    }
}


void partition() {
    vector<vertex> temp[thread_num];
    for (int i = 0; i < graph.size(); i++)
        temp[i % thread_num].push_back(graph[i]);
    for (int i = 0; i < thread_num; i++)
        //建立子线程，实现详见下文
        subthreads.push_back(thread(subthread_func,temp[i],i));
}



//线程函数，执行“部分算法”


void add_edge(edge e,map<int,int>& i,map<int,vector<int>>& rev_i) {
    mst.push_back(e);
    //cid 记录的应该是component
    int cid1 = i[e.v1],cid2 = i[e.v2];
    if (cid1 == -1 && cid2 == -1) {
        int cid = rev_i.size() + 1;
        vector<int> temp;
        temp.push_back(e.v1);
        temp.push_back(e.v2);
        rev_i[cid] = temp;
        i[e.v1] = cid;
        i[e.v2] = cid;
    }
    else if (cid1 == -1) {
        i[e.v1] = cid2;
        rev_i[cid2].push_back(e.v1);
    }
    else if (cid2 == -1) {
        i[e.v2] = cid1;
        rev_i[cid1].push_back(e.v2);
    }
    // merge
    else if (rev_i[cid1].size() <= rev_i[cid2].size()) {
        for (int k = 0; k < rev_i[cid1].size(); k++) {
            i[rev_i[cid1][k]] = cid2;
            rev_i[cid2].push_back(rev_i[cid1][k]);
        }
        rev_i.erase(cid1);
    }
    else {
        for (int k = 0; k < rev_i[cid2].size(); k++) {
            i[rev_i[cid2][k]] = cid1;
            rev_i[cid1].push_back(rev_i[cid2][k]);
        }
        rev_i.erase(cid2);
    }
}

void kruskal()
{
    map<int,int> index;
    map<int,vector<int>> rev_index;
    for (int i = 0; i < graph.size(); i++) {
        index[i + 1] = -1;
    }
    while (mst.size() < graph.size() - 1) {
        unique_lock<mutex> lk(mut);
        if (edge_queue.empty())
            break;
        pair<int,edge> temp = *(edge_queue.begin());
        edge_queue.erase(edge_queue.begin());
        lk.unlock();
        cond_v[temp.second.tid].notify_all();
        // 不会成环
        if (index[temp.second.v1] != index[temp.second.v2] || index[temp.second.v1] == -1)
            add_edge(temp.second,index,rev_index);
    }

    isfinished = true;

    for (int i = 0; i < thread_num; i++) {
        // 全部唤醒，部分算法退出
        cond_v[i].notify_all();
        subthreads[i].join();
    }
}


void output()
{
    for (int i = 0; i < mst.size(); i++)
        cout << mst[i].v1 << " -> " << mst[i].v2 << endl;
}

int main()
{
    construct();//构造图的邻接表
    LARGE_INTEGER t1,t2,tc;
    QueryPerformanceFrequency(&tc);
    QueryPerformanceCounter(&t1);
    partition();//图的划分，创建并发进程，执行“部分算法”
    kruskal();//全局进程执行“仲裁算法”
    QueryPerformanceCounter(&t2);
    std::cout << "time: " << (double)(t2.QuadPart-t1.QuadPart) * 1000 / tc.QuadPart << endl;  //单位为ms
    output();//结果输出
    return 0;
}