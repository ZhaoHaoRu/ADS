#include "AVLTree.h"

AVLNode* AVLTree::search(const int key, AVLNode **prev)
{
    AVLNode *t = root;
    while(t != nullptr && t->key != key){
        *prev = t;
        if(t->key > key)
            t = t->left;
        else
            t = t->right;
    }
//    if(*prev)
//        cout << (*prev)->val << endl;
    if(t == nullptr)
        return nullptr;
    else 
        return t;
}

bool AVLTree::isBalanced(AVLNode *node)
{
    if(node == nullptr || (!node->left && !node->right))
        return true;
    else if(node->left == nullptr){
        if(node->right->height <= 1)
            return true;
        else
            return false;
    }
    else if(node->right == nullptr){
        if(node->left->height <= 1)
            return true;
        else
            return false;
    }
    else{
        int sub = node->left->height - node->right->height;
        if(sub <= -2 || sub >= 2)
            return false;
        else 
            return true;
    }
}

void AVLTree::updateHeight(AVLNode *node)
{
    if(node == nullptr)
        return;
    if(!node->left && !node->right){
        node->height = 1;
        return;
    }
    if(!node->right){
        node->height = node->left->height + 1;
        return;
    }
    if(!node->left){
        node->height = node->right->height + 1;
        return;
    } 
    else{
        node->height = max(node->right->height, node->left->height) + 1;
    }
}

bool AVLTree::isLChild(AVLNode *node)
{
    if(!node->parent)
        return false;
    AVLNode *p = node->parent;
    if(p->left && p->left == node)
        return true;
    else 
        return false;
}

bool AVLTree::isRChild(AVLNode *node)
{
    if(!node->parent)
        return false;
    AVLNode *p = node->parent;
    if(p->right && p->right == node)
        return true;
    else 
        return false;
}

AVLNode *AVLTree::tallChild(AVLNode *node)
{
    if(node == nullptr || (!node->right && !node->left))
        return nullptr;
    if(!node->left)
        return node->right;
    if(!node->right)
        return node->left;
    else{
        if(node->left->height > node->right->height)
            return node->left;
        else if(node->right->height >  node->left->height)
            return node->right;
        else{
            if(isLChild(node))
                return node->left;
            else
                return node->right;
        }
    }
}

 AVLNode *AVLTree::connect34(AVLNode *a, AVLNode *b, AVLNode *c, AVLNode *t0, AVLNode *t1, AVLNode *t2, AVLNode *t3)
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

AVLNode *AVLTree::rotateAt(AVLNode *v)
{
    ++rotateCount;
    AVLNode *p = v->parent, *g = nullptr;
    if(p->parent)
        g = p->parent;
    if(isLChild(p)){
        if(isLChild(v)){
            p->parent = g->parent;
            return connect34(v, p, g, v->left, v->right, p->right, g->right); 
        }
        else{
            v->parent = g->parent;
            return connect34(p, v, g, p->left, v->left, v->right, g->right);
        }
    }
    else{
        if(isRChild(v)){
            p->parent = g->parent;
            return connect34(g, p, v, g->left, p->left, v->left, v->right);
        }
        else{
            v->parent = g->parent;
            return connect34(g, v, p, g->left, v->left, v->right, p->right);
        }
    }
}

AVLNode* AVLTree::insert(int key, int val)
{
    if(root == nullptr){
        root = new AVLNode(key, val);
        return root;
    }
    AVLNode *prev;
    AVLNode *res = search(key, &prev);
    if(res)
        return res;
    AVLNode *newNode = new AVLNode(key, val, prev);
    if(key < prev->key)
        prev->left = newNode;
    else
        prev->right = newNode;
    for(AVLNode *g = prev; g; g = g->parent){
        if(!isBalanced(g)){
            if(g->parent){
                AVLNode *ans = g->parent;
                if(isLChild(g))
                    ans->left = rotateAt(tallChild(tallChild(g)));
                else
                    ans->right = rotateAt(tallChild(tallChild(g)));
            }
            else {
                root = rotateAt(tallChild(tallChild(g)));
                updateHeight(root);
            }
            break;
        }
        else
            updateHeight(g);
    }
    return newNode;
}
