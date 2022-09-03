//
// Created by Lenovo on 2022/4/8.
//

#ifndef RBTREE_RBTREE_H
#define RBTREE_RBTREE_H

#include <algorithm>
#include <iostream>
using namespace std;


enum colorT{red, black};
struct RBNode{
    int key;
    int val;
    RBNode *right;
    RBNode *left;
    RBNode *parent;
    int height;
    colorT color;

    RBNode(const int k = 0, const int v = 0, colorT c = red, RBNode *p = nullptr, int h = 0, RBNode *r = nullptr, RBNode *l = nullptr){
        key = k;
        val = v;
        parent = p;
        height = h;
        color = c;
        right = r;
        left = l;
    }
};

class RBTree {
    RBNode *root;


    bool isBlack(RBNode *node);
    bool isLChild(RBNode *node);
    RBNode* uncle(RBNode *node);
    RBNode *connect34(RBNode *a, RBNode *b, RBNode *c, RBNode *t0, RBNode *t1, RBNode *t2, RBNode *t3);
    RBNode *rotateAt(RBNode *v);

public:
    int rotateCount = 0;
    void insert(const int key, const int val);
    RBNode* search(const int key, RBNode **prev);
    void solveDoubleRed(RBNode *node);
    int updateHeight(RBNode *node);

    ~RBTree();

    RBTree(RBNode *r = nullptr);
};


#endif //RBTREE_RBTREE_H
