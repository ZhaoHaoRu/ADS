#include "ScapegoatTree.h"
#include <iostream>
#include <cmath>

ScapegoatTree::ScapegoatTree()
{
    root = nullptr;
    rebalanceTimes = 0;
    height = 0;
    size = 0;
    maxSize = 0;
}

bool ScapegoatTree::isHeightBalanced()
{
    double bound = log(size) / log(1 / ALPHA);
    return height <= bound;
}

bool ScapegoatTree::isWeightBalanced(SGNode *node)
{
    if(node == nullptr)
        return true;
    int Lsize = UpdateSingleSize(node->left);
    int Rsize = UpdateSingleSize(node->right);
    int size = Lsize + Rsize + 1;
    int tmpsize = max(Lsize, Rsize);
    if((double)tmpsize / size <= ALPHA)
        return true;
    else
        return false;
}

int ScapegoatTree::GetRebalanceTimes()
{
    // TODO
    return rebalanceTimes;
}

void ScapegoatTree::Search(int key)
{
    if(root == nullptr){
        cout << "Not Found" << endl;
        return;
    }
    int depth = 0;
    SGNode *tmp = root;
    while(tmp){
        if(tmp->key == key){
            cout << depth << endl;
            return;
        }
        else if(tmp->key < key){
            ++depth;
            tmp = tmp->right;
        }
        else{
            ++depth;
            tmp = tmp->left;
        }
    }
    cout << "Not Found" << endl;
//    return -1;
}

bool ScapegoatTree::FindParent(int key, SGNode **prev)
{
    SGNode *tmp = root;
    while(tmp){
        if(key == tmp->key){
            return false;
        }
        else if(key < tmp->key){
            *prev = tmp;
            tmp = tmp->left;
        }
        else{
            *prev = tmp;
            tmp = tmp->right;
        }
    }
    return true;
}

void ScapegoatTree::midOrder(vector<int> &arr, SGNode *node)
{
    if(node == nullptr)
        return;
    midOrder(arr, node->left);
    arr.emplace_back(node->key);
    midOrder(arr, node->right);
    delete node;
}

SGNode *ScapegoatTree::BuildTree(vector<int> &arr, int left, int right, int height)
{
    if(left > right)
        return nullptr;
    int mid = left + (right - left) / 2;
    SGNode *node = new SGNode(arr[mid], height);
    node->left = BuildTree(arr, left, mid - 1, height + 1);
    node->right = BuildTree(arr, mid + 1, right, height + 1);
    if(node->left)
        node->left->parent = node;
    if(node->right)
        node->right->parent = node;
    return node;
}


//Update Rsize and Lsize
void ScapegoatTree::Update(SGNode *node)
{
    if(node == nullptr)
        return;
    if(node->left == nullptr && node->right == nullptr){
        node->Lsize = 0;
        node->Rsize = 0;
        return;
    }
    Update(node->left);
    Update(node->right);
    if(node->left != nullptr)
        node->Lsize = node->left->countSize();
    else
        node->Lsize = 0;
    if(node->right != nullptr)
        node->Rsize = node->right->countSize();
    else
        node->Rsize = 0;
    return;
}

int ScapegoatTree::UpdateHeight(SGNode *node)
{
    if(node == nullptr)
        return 0;
    return 1 + max(UpdateHeight(node->left), UpdateHeight(node->right));
}

vector<vector<int>> ScapegoatTree::levelOrder(){
    vector<vector<int>> result;
    if(root == nullptr)
        return result;
    queue<SGNode*> que;
    que.push(root);
    while(!que.empty()) {
        int n = que.size();
        vector<int> arr;
        for (int i = 0; i < n; ++i) {
            SGNode *cur = que.front();
            arr.emplace_back(cur->key);
            que.pop();
            if (cur->left) {
                que.push(cur->left);
            }
            if (cur->right) {
                que.push(cur->right);
            }
        }
        result.emplace_back(arr);
    }
    return result;
}

SGNode *ScapegoatTree::reBuild(SGNode *node)
{
    if(node == nullptr)
        return nullptr;
    vector<int> arr;
    int height = node->height;
    midOrder(arr, node);
    SGNode *tmp = BuildTree(arr, 0, arr.size() - 1, height);
    return tmp;
}


int ScapegoatTree::UpdateSingleSize(SGNode *node)
{
    if(node == nullptr)
        return 0;
    return 1 + UpdateSingleSize(node->left) + UpdateSingleSize(node->right);
}

void ScapegoatTree::UpdateSize(SGNode *node)
{
    SGNode *prev = node->parent, *tmp = node;
    while(prev != nullptr){
        if(tmp == prev->left){
            prev->Lsize = tmp->countSize();
            int tmpSize = UpdateSingleSize(tmp);
        }
        else{
            prev->Rsize = tmp->countSize();
            int tmpSize = UpdateSingleSize(tmp);
        }
        tmp = prev;
        prev = prev->parent;
    }
}

void ScapegoatTree::Insert(int key)
{
    int preHeight = height;
    if(root == nullptr){
        root = new SGNode(key);
        ++size;
        maxSize = max(size, maxSize);
        height = 0;
        return;
    }

    //find the parent node and then create new node
    SGNode *prev = nullptr;
    SGNode *newNode = nullptr;
    if(!FindParent(key, &prev)){
        if(!prev)
            return;
        if(prev->key < key)
            newNode = prev->right;
        else
            newNode = prev->left;
    }
    else{
        newNode = new SGNode(key, 0, prev);
        int newHeight = newNode->height;
        if(prev == nullptr)
            exit(-1);

        //update parent node
        if(key < prev->key){
            prev->left = newNode;
            prev->Lsize = newNode->countSize();
        }
        else{
            prev->right = newNode;
            prev->Rsize = newNode->countSize();
        }
        UpdateSize(newNode);
        ++size;
    }



    //update the tree's information
    maxSize = max(size, maxSize);
    height = max(height, newNode->height);
    int height2 = UpdateHeight(root) - 1;
    if(height != height2){
        cout << "height error" << endl;
        exit(-1);
    }


    //judge height balance
    if(isHeightBalanced() || (preHeight == height && newNode->height != height))
        return;

    //begin to rebuild
    //notice the information need to update again
    ++rebalanceTimes;
    SGNode *Scapegoat = newNode->parent;
    while(Scapegoat != nullptr){
        if(!isWeightBalanced(Scapegoat))
            break;
        Scapegoat = Scapegoat->parent;
    }
    if(Scapegoat == nullptr)
        return;


    prev = Scapegoat->parent;

    //update the former tree
    SGNode *tmp = reBuild(Scapegoat);
    if(prev == nullptr) {
        root = tmp;
    }
    else if(prev && prev->key > tmp->key){
        prev->left = tmp;
        if(prev->left)
            prev->left->parent = prev;
    }
    else{
        prev->right = tmp;
        if(prev->right)
            prev->right->parent = prev;
    }
    Update(root);
    height = UpdateHeight(root) - 1;
}

bool ScapegoatTree::DelNode(SGNode *&node, int key)
{
    if(node == nullptr){
        return false;
    }
    if(key < node->key){
        DelNode(node->left, key);
    }
    else if(key > node->key)
        DelNode(node->right, key);
    else if(node->left != nullptr && node->right != nullptr)
    {
        SGNode *tmp = node->right;
        while(tmp->left != nullptr)
            tmp = tmp->left;
        node->key = tmp->key;
        DelNode(node->right, tmp->key);
        return true;
    }
    else{
        SGNode *del = node;
        SGNode *p = node->parent;
        bool isLeft;
        if(p && p->left == node){
            isLeft = true;
        }
        else{
            isLeft = false;
        }
        node = (node->left == nullptr) ? node->right : node->left;
        if(node != nullptr)
            node->parent = p;
        if(p && isLeft)
            p->left = node;
        else if(p)
            p->right = node;
        delete del;
        HandleHeight(node);
        return true;
    }
}

void ScapegoatTree::HandleHeight(SGNode *node)
{
    if(node == nullptr)
        return;
    --node->height;
    HandleHeight(node->left);
    HandleHeight(node->right);
}

void ScapegoatTree::Delete(int key)
{
    // TODO
    if(root == nullptr){
        ++rebalanceTimes;
        return;
    }
    SGNode *prev = root;
    if(!DelNode(prev, key))
        return;

    Update(root);
    height = UpdateHeight(root) - 1;
    --size;


    //judge if need rebuild or not
    if(size <= ALPHA * maxSize){
        ++rebalanceTimes;
        root = reBuild(root);
        height = UpdateHeight(root) - 1;
        Update(root);
        maxSize = size;
    }
}
