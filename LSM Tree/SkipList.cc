#include <iostream>
#include <stdlib.h>

#include "SkipList.h"

SkipList::SkipList(){
    head = new SKNode(0, "", SKNodeType::HEAD);
    NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
    listSize = 0;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        head->forwards[i] = NIL;
    }
    remainSpace = 2086880;
    // std::cout << "get here" << std::endl;
}

double SkipList::my_rand()
{
    // s = (16807 * s) % 2147483647ULL;
    // return (s + 0.0) / 2147483647ULL;
    srand(time(NULL));
    double randomNum = ((double)rand())/RAND_MAX;
    return randomNum;
}

int SkipList::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

bool SkipList::Insert(uint64_t key, std::string value)
{
    std::vector<SKNode*> update(MAX_LEVEL);
    SKNode *x = head;
    // std::cout << "skipList insert !" << std::endl;
    for(int i = MAX_LEVEL-1; i >= 0; --i){
        while(x->forwards[i] != NIL && x->forwards[i]->key < key){
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = x->forwards[0];
    if(x != NIL && x->key == key){
        //string最后的斜杠零还需要考虑吗？
        if((x->val).length() > value.length() || remainSpace >= -(x->val).length() + value.length()){
            remainSpace = remainSpace + (x->val).length() - value.length();
            x->val = value;
            ++listSize;
            // std::cout << "remainSpace at 55: " << remainSpace << std::endl; 
            return true;
        }
        else
            return false;
    }
    else{
        if(remainSpace < 12 + value.length()){
            return false;
        }
        // std::cout << "remainSpace: " << remainSpace <<" " << key << std::endl; 
        remainSpace = remainSpace - 12 - value.length();
        int level = randomLevel();
        SKNode *newNode = new SKNode(key, value, SKNodeType::NORMAL);
        for(int i = 0; i < level; ++i){
            newNode->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = newNode;
        }
        ++listSize;
    }
    // std::cout << "remainSpace: " << remainSpace << std::endl; 
    return true;
}

bool SkipList::Search(uint64_t key, std::string &value, bool &hasDel)
{
    // if(key == 1)
    //     int h = 1;
    // bool isfound = false;
    SKNode *tmp = head;
    for(int i = MAX_LEVEL - 1; i >= 0; --i){
        if(tmp->forwards[i] == NIL){
            continue;
        }
        while(tmp->forwards[i] != NIL && tmp->forwards[i]->key < key){
            tmp = tmp->forwards[i];
        }
    }
    tmp = tmp->forwards[0];
    if(tmp != NIL && tmp->key == key && tmp->val != "~DELETED~"){
        value = tmp->val;
        return true;
    }
    else{
        if(tmp->val == "~DELETED~" && tmp->key == key)
            hasDel = true;
        return false;
    }
}

bool SkipList::Delete(uint64_t key)
{
    std::vector<SKNode*> update(MAX_LEVEL);
    SKNode *x = head;
    for(int i = MAX_LEVEL-1; i >= 0; --i){
        while(x->forwards[i] != NIL && x->forwards[i]->key < key){
            x = x->forwards[i];
        }
        update[i] = x;
    }
    bool isDeleted = false;
    x = x->forwards[0];
    if(x != NIL && x->key == key){
        if(x->val != "~DELETED~")
            isDeleted = true;
        for(int i = 0; i < 8; ++i){
            if(update[i]->forwards[i] != x)
                break;
            update[i]->forwards[i] = x->forwards[i];
        }
        x->val = "~DELETED~";
    }
    return isDeleted;
}

void SkipList::Display()
{
    for (int i = MAX_LEVEL - 1; i >= 0; --i)
    {
        std::cout << "Level " << i + 1 << ":h";
        SKNode *node = head->forwards[i];
        while (node->type != SKNodeType::NIL)
        {
            std::cout << "-->(" << node->key << "," << node->val << ")";
            node = node->forwards[i];
        }

        std::cout << "-->N" << std::endl;
    }
}

std::list<std::pair<uint64_t, std::string>> SkipList::Scan(uint64_t key_start, uint64_t key_end){
    int count = 0;
    std::list<std::pair<uint64_t, std::string>> result;
    SKNode *tmp = head;
    for(int i = MAX_LEVEL - 1; i >= 0; --i){
        if(tmp->forwards[i] == NIL){
            continue;
        }
        while(tmp->forwards[i] != NIL && tmp->forwards[i]->key < key_start){
            tmp = tmp->forwards[i];
        }
    }
    tmp = tmp->forwards[0];
    while(tmp != NIL && tmp->key >= key_start && tmp->key <= key_end){
        ++count;
        result.push_back(std::pair<uint64_t, std::string>(tmp->key, tmp->val));
        tmp = tmp->forwards[0];
    }
    return result;
} 

SKNode *SkipList::ScanHead(uint64_t key1, uint64_t key2)
{
    // std::list<std::pair<uint64_t, std::string>> result;
    SKNode *result = nullptr;
    SKNode *tmp = head;
    for(int i = MAX_LEVEL - 1; i >= 0; --i){
        if(tmp->forwards[i] == NIL){
            continue;
        }
        while(tmp->forwards[i] != NIL && tmp->forwards[i]->key < key1){
            tmp = tmp->forwards[i];
        }
    }
    tmp = tmp->forwards[0];
    if(tmp != NIL && tmp->key >= key1 && tmp->key <= key2){
       result = tmp;
    }
    return result;
}
void SkipList::WriteOut(uint64_t &min, uint64_t &max, uint32_t &size, std::vector<std::pair<uint64_t, std::string>> &li)
{
    SKNode *tmp = head->forwards[0];
    while(tmp != NIL){
        ++size;
        if(size == 1)
            min = tmp->key;
        max = tmp->key;
        li.push_back(std::pair<uint64_t, std::string>(tmp->key, tmp->val));
        tmp = tmp->forwards[0];
    }
}

void SkipList::Clear()
{
    SKNode *n1 = head;
    SKNode *n2;
    while (n1->forwards[0] != NIL)
    {
        n2 = n1->forwards[0];
        n1->forwards[0] = n2->forwards[0];
        delete n2;
    }
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        head->forwards[i] = NIL;
    }
}