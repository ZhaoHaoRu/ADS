#include <iostream>
#include <utility>
#include <algorithm>

using namespace std;

struct AVLNode{
    int key, val;
    AVLNode *left;
    AVLNode *parent;
    AVLNode *right;
    int height;

    AVLNode(int k, int v, AVLNode *p = nullptr, AVLNode *lnode = nullptr, AVLNode *rnode = nullptr, int h = 1)
    {
        key = k;
        val = v;
        parent = p;
        left = lnode;
        right = rnode;
        height = h;
    }
};

class AVLTree{
private:
    AVLNode *root;
    AVLNode *connect34(AVLNode *a, AVLNode *b, AVLNode *c, AVLNode *t0, AVLNode *t1, AVLNode *t2, AVLNode *t3);
    AVLNode *rotateAt(AVLNode *v);
    bool isBalanced(AVLNode *node);
    bool isLChild(AVLNode *node);
    bool isRChild(AVLNode *node);
    AVLNode *tallChild(AVLNode *node);
    void updateHeight(AVLNode *node);
public:
    int rotateCount = 0;
    AVLTree(){
        root = nullptr;
    }
    ~AVLTree();
    AVLNode* insert(int key, int val);
    AVLNode *search(const int key, AVLNode **prev);
};