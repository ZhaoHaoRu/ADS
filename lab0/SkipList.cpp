#include <iostream>
#include <stdlib.h>

#include "SkipList.h"

double SkipList::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
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

void SkipList::Insert(int key, int value)
{
    vector<SKNode*> update(8);
    SKNode *x = head;
    for(int i = MAX_LEVEL-1; i >= 0; --i){
        while(x->forwards[i] != NIL && x->forwards[i]->key < key){
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = x->forwards[0];
    if(x != NIL && x->key == key){
        x->val = value;
        return;
    }
    else{
        int level = randomLevel();
        SKNode *newNode = new SKNode(key, value, SKNodeType::NORMAL);
        for(int i = 0; i < level; ++i){
            newNode->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = newNode;
        }
    }
    return;
}

void SkipList::Search(int key)
{
    bool isfound = false;
    SKNode *tmp = head;
    for(int i = MAX_LEVEL - 1; i >= 0; --i){
        if(tmp->forwards[i] == NIL){
            if(tmp == head)
                std::cout << i + 1 << "," << "h" << " ";
            else
                std::cout << i + 1 << "," << tmp->key << " ";
            continue;
        }
        while(tmp->forwards[i] != NIL && tmp->forwards[i]->key < key){
            if(tmp == head)
                std::cout << i + 1 << "," << "h" << " ";
            else 
                std::cout << i + 1 << "," << tmp->key << " ";
            tmp = tmp->forwards[i];
        }
        if(tmp == head)
            std::cout << i + 1 << "," << "h" << " ";
        else 
            std::cout << i + 1 << "," << tmp->key << " ";
    }
    tmp = tmp->forwards[0];
    if(tmp != NIL)
        std::cout << 1 << "," << tmp->key << " ";
    else
        std::cout << 1 << ",N ";
    if(tmp != NIL && tmp->key == key){
        std::cout << tmp->val << endl;
        return;
    }
    else{
        std::cout << "Not Found" << endl;
        return;
    }
}

void SkipList::Delete(int key)
{
    vector<SKNode*> update(8);
    SKNode *x = head;
    for(int i = MAX_LEVEL-1; i >= 0; --i){
        while(x->forwards[i] != NIL && x->forwards[i]->key < key){
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = x->forwards[0];
    if(x != NIL && x->key == key){
        for(int i = 0; i < 8; ++i){
            if(update[i]->forwards[i] != x)
                break;
            update[i]->forwards[i] = x->forwards[i];
        }
        delete x;
    }
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