#include <iostream>

#include "HashTable.h"

int HashTable::hash(int key)
{
    return key % BUCKET_SIZE;
}

void HashTable::Insert(int key, int value)
{
    // TODO
    int bucket_id = hash(key);
    // cout << "bucker_id:" << bucket_id << endl;
    HashNode* bucket_node = bucket[bucket_id];
    bool isInsert = false;
    if(bucket_node == nullptr){
        // cout << "node init" << endl;
        bucket[bucket_id] = new HashNode(key, value);
        return;
    }
    else if (bucket_node->key == key){
        bucket[bucket_id]->val = value;
        return;
    }
    while(bucket_node->next != nullptr){
        // cout << bucket_node->next->key << endl;
        if(bucket_node->next->key == key){
            bucket_node->next->val = value;
            // cout << "change value: "<< value << endl;
            isInsert = true;
            break; 
        }
        bucket_node = bucket_node->next;
    }
    if(!isInsert){
        // cout << "insert" << key <<" "<< value<< endl;
        bucket_node->next = new HashNode(key, value);
        //cout << "bucket_node:" << bucket_node->key << " " << bucket_node->val << endl; 
    }
}

void HashTable::Search(int key)
{
    // TODO
    int bucket_id = hash(key);
    HashNode* bucket_node = bucket[bucket_id];
    bool isFound = false;
    int num = 0, val = 0;
    while(bucket_node != nullptr){
        if(bucket_node->key == key){
            isFound = true;
            val = bucket_node->val;
            break;
        }
        bucket_node = bucket_node->next;
        ++num;
    }
    if(!isFound)
        cout << "Not Found" << endl;
    else
        cout << "bucket " << bucket_id << " index " << num << " key " << key << " value " << val << endl;
    
}

void HashTable::Delete(int key)
{
    // TODO
    int bucket_id = hash(key);
    HashNode* bucket_node = bucket[bucket_id];
    if(bucket_node == nullptr)
        return;
    else if(bucket_node->key == key){
        bucket[bucket_id] = bucket_node->next;
        delete bucket_node;
        return;
    }
    while(bucket_node->next != nullptr){
        if(bucket_node->next->key == key){
            HashNode *del = bucket_node->next;
            bucket_node->next = bucket_node->next->next;
            delete del;
            return;
        }
        bucket_node = bucket_node->next;
    }
}

void HashTable::Display()
{
    for (int i = 0; i < BUCKET_SIZE; ++i)
    {
        std::cout << "|Bucket " << i << "|";
        HashNode *node = bucket[i];
        while (node)
        {
            std::cout << "-->(" << node->key << "," << node->val << ")";
            node = node->next;
        }
        std::cout << "-->" << std::endl;
    }
}