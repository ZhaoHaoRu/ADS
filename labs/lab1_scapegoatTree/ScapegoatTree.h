#ifndef LAB1_SCAPEGOATTREE_H
#define LAB1_SCAPEGOATTREE_H


#include <vector>
#include <iostream>
#include <queue>
#include <algorithm>
using namespace std;

#define ALPHA 0.6

struct SGNode
{
    int height;
    int key;
    SGNode *left;
    SGNode *right;
    SGNode *parent;
    int Lsize;
    int Rsize;

    SGNode(int k = 0, int h = 0, SGNode *p = nullptr, SGNode *l = nullptr, SGNode *r = nullptr)
    {
        key = k;
        parent = p;
        if(h != 0)
            height = h;
        else if(parent != nullptr){
            height = parent->height + 1;
        }
        else
            height = 0;
        left = l;
        right = r;
        if(l == nullptr)
            Lsize = 0;
        else
            Lsize = l->Lsize + l->Rsize + 1;
        if(r == nullptr)
            Rsize = 0;
        else
            Rsize = r->Lsize + r->Rsize + 1;
    }

    int countSize(){
        return Lsize + Rsize + 1;
    }
};
class ScapegoatTree
{
private:
    SGNode *root;
    int rebalanceTimes;
    int height;
    int size;
    int maxSize;

    bool FindParent(int key, SGNode **prev);
    bool isHeightBalanced();
    bool isWeightBalanced(SGNode *node);
    void midOrder(vector<int> &arr, SGNode *node);
    SGNode *BuildTree(vector<int> &arr, int left, int right, int height);
    SGNode *reBuild(SGNode *node);
    void Update(SGNode *node);
    int UpdateSingleSize(SGNode *node);
    void UpdateSize(SGNode *node);
    void HandleHeight(SGNode *node);
    int UpdateHeight(SGNode *node);
    bool DelNode(SGNode *&node, int key);

public:
    ScapegoatTree();
    ~ScapegoatTree() {}

    /**
     * @brief Get the Rebalance Times
     *
     * @return int
     */
    int GetRebalanceTimes();

    /**
     * @brief Look up a key in scapegoat tree. Same as trivial binary search tree
     *
     * @param key
     */
    void Search(int key);

    /**
     * @brief Insert a new node into the tree. If the key is already in the tree, then do nothing
     *
     * @param key
     */
    void Insert(int key);

    /**
     * @brief Delete a node of the tree. If the key is not in the tree, do nothing.
     *
     * @param key
     */
    void Delete(int key);

    vector<vector<int>> levelOrder();
};


#endif //LAB1_SCAPEGOATTREE_H
