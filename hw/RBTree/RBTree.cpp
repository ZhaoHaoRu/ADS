//
// Created by Lenovo on 2022/4/8.
//

#include "RBTree.h"
RBTree::RBTree(RBNode *r)
{
    root = r;
}

RBNode* RBTree::search(const int key, RBNode **prev)
{
    if(root == nullptr)
        return nullptr;
    RBNode* tmp = root;
    *prev = tmp;
    while(tmp != nullptr){
        if(tmp->key == key)
            return tmp;
        else if(tmp->key > key) {
            *prev = tmp;
            tmp = tmp->left;
        }
        else {
            *prev = tmp;
            tmp = tmp->right;
        }
    }
//    if(*prev != nullptr){
//        cout << "not nullptr" << endl;
//    }
    return nullptr;
}

bool RBTree::isBlack(RBNode *node)
{
    return node == nullptr || node->color == black;
}

//对于根节点一律默认左孩子
bool RBTree::isLChild(RBNode *node) {
    if(!node->parent)
        return true;
    return node->parent->left == node;
}

RBNode* RBTree::uncle(RBNode *p){
    if(p->parent == nullptr)
        return nullptr;
    if(isLChild(p))
        return p->parent->right;
    else
        return p->parent->left;
}

int RBTree::updateHeight(RBNode *node)
{
    if(node == nullptr)
        return 1;
    if(!node->left && !node->right)
        node->height = 1;
    else if(!node->left)
        node->height = node->right->height;
    else if(!node->right)
        node->height = node->left->height;
    else
        node->height = max(node->left->height, node->right->height);
    return isBlack(node) ? ++node->height:node->height;
}

RBNode *RBTree::connect34(RBNode *a, RBNode *b, RBNode *c, RBNode *t0, RBNode *t1, RBNode *t2, RBNode *t3)
{
    a->left = t0;
    if(t0)
        t0->parent = a;
    a->right = t1;
    if(t1)
        t1->parent = a;
    updateHeight(a);
    c->left = t2;
    if(t2)
        t2->parent = c;
    c->right = t3;
    if(t3)
        t3->parent = c;
    updateHeight(c);
    b->left = a;
    a->parent = b;
    b->right = c;
    c->parent = b;
    updateHeight(b);
    return b;
}

RBNode *RBTree::rotateAt(RBNode *v) {
    ++rotateCount;
    RBNode *p = v->parent, *g = nullptr;
    if (p->parent)
        g = p->parent;
    if (isLChild(p)) {
        if (isLChild(v)) {
            p->parent = g->parent;
            return connect34(v, p, g, v->left, v->right, p->right, g->right);
        } else {
            v->parent = g->parent;
            return connect34(p, v, g, p->left, v->left, v->right, g->right);
        }
    } else {
        if (!isLChild(v)) {
            p->parent = g->parent;
            return connect34(g, p, v, g->left, p->left, v->left, v->right);
        } else {
            v->parent = g->parent;
            return connect34(g, v, p, g->left, v->left, v->right, p->right);
        }
    }
}

void RBTree::insert(const int key, const int val)
{
    if(root == nullptr) {
        root = new RBNode(key, val, black);
        return;
    }
    RBNode *prev = nullptr;
    RBNode *node = search(key, &prev);
    if(node)
        return;
    RBNode *newNode = new RBNode(key, val, red, prev);
    if(key < prev->key)
        prev->left = newNode;
    else
        prev->right = newNode;
    solveDoubleRed(newNode);
//    cout << "insert: " << key << " " << val << endl;
    return;
}

void RBTree::solveDoubleRed(RBNode *node)
{
    if(node->parent == nullptr){
        node->color = black;
        ++node->height;
        return;
    }
    RBNode *p = node->parent;
    if(p->color == black)
        return;
    RBNode *g = p->parent;
    RBNode *u = uncle(p);
    if(isBlack(u)){
        if(isLChild(node) && isLChild(p))
            p->color = black;
        else if(!isLChild(node) && !isLChild(p))
            p->color = black;
        else
            node->color = black;
        if(g)
            g->color = red;
        if(g->parent){
            RBNode *ans = g->parent;
            bool isLeft = isLChild(g);
            RBNode *res = rotateAt(node);
            if(isLeft)
                ans->left = res;
            else
                ans->right = res;
            res->parent = ans;
        }
        else{
            root = rotateAt(node);
            updateHeight(root);
        }
    }
    else{
        p->color = black;
        ++p->height;
        u->color = black;
        ++u->height;
        if(g->parent != nullptr)
            g->color = red;
        solveDoubleRed(g);
    }
}