//
// Created by Lenovo on 2022/5/5.
//
#include <string>
#include <iostream>
#include <windows.h>
#include <vector>

using namespace std;

int strStr(string haystack, string needle) {
    int n1 = haystack.length(), n2 = needle.length();
    if(n2 == 0)
        return 0;
    else if(n1 < n2)
        return -1;
    int j = 0;
    for(int i = 0;i < n1; ++i){
        if(haystack[i] == needle[0]){
            for(j = 1;j < n2; ++j){
                if(haystack[j + i] != needle[j])
                    break;
            }
            if(j == n2)
                return i;
        }
    }
    return -1;
}


void calNext(const string &needle, vector<int> &next)
{
    int len = needle.length();
    for(int j = 1, p = -1; j < len; ++j){
        while(p > -1 && needle[j] != needle[p + 1]){
            p = next[p];    // 如果下一位不同，往前回溯
        }
        if(needle[p + 1] == needle[j])
            ++p;    // 如果下一位相同，更新相同的最大前缀和最大后缀长
        next[j] = p;
    }
}

int strStr2(string haystack, string needle) {
    int k = -1, n = haystack.length(), p = needle.length();
    if(p == 0)
        return 0;
    vector<int> next(p, -1);
    calNext(needle, next);
    for(int i = 0; i < n; ++i){
        while(k > -1 && needle[k + 1] != haystack[i])
            k = next[k];
        if(needle[k + 1] == haystack[i])
            ++k;
        if(k == p - 1)
            return i - p + 1;
    }
    return -1;
}

int main(){
    LARGE_INTEGER t1,t2,tc;
//    string test1="81167200584709191421";
//    string test1="21482804758227577539935853886285365382802764296643";
    string test1= "88551259126598899221146812553484777857751513783013454816691927645007539625446425584685915959529828187012884432148724254376491274155096093409209088485439707399738023593813969262986930738943450713568239";
    string test2="9692629869";
    QueryPerformanceFrequency(&tc);
    QueryPerformanceCounter(&t1);
    strStr(test1, test2);
    QueryPerformanceCounter(&t2);
    cout << (double)(t2.QuadPart-t1.QuadPart) << endl;

    QueryPerformanceCounter(&t1);
    strStr2(test1, test2);
    QueryPerformanceCounter(&t2);
    cout << (double)(t2.QuadPart-t1.QuadPart) << endl;
}