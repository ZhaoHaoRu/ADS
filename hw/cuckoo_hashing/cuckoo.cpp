//
// Created by Haoru Zhao on 2022/5/21.
//

#include "cuckoo.h"


Cuckoo::Cuckoo(){
    memset(T, 0, sizeof(KeyType) * SIZE);
}

Cuckoo::~Cuckoo(){};

int Cuckoo::hash1(const KeyType &key){
    assert(SIZE != 0);
    int half_siz = SIZE / 2;
    return key%half_siz;
}

int Cuckoo::hash2(const KeyType &key){
    assert(SIZE != 0);
    int half_siz = SIZE / 2;
    return key % half_siz + half_siz;
}

// find key by hash func 1 in T, exist return key otherwise 0
KeyType Cuckoo::get1(const KeyType &key){
    return (T[hash1(key)] == key)?key:0;
}

// find key by hash func 2 in T, exist return key otherwise 0
KeyType Cuckoo::get2(const KeyType &key){
    return (T[hash2(key)] == key)?key:0;
}

KeyType Cuckoo::get(const KeyType &key){
    // 0 is reserved for null, invalid input
    if(key == 0){
        printf("invalid key\n");
        return 0;
    }
    KeyType result = get1(key);
    if(result == 0){
        result = get2(key);
    }
    return result;
}

template <typename T>
inline void swap(T* a, T* b) {
    assert(a != NULL && b != NULL);
    T tmp = *a;
    *a = *b;
    *b = tmp;
}

void Cuckoo::put(const KeyType &key){
    if(key == 0){
        printf("invalid key\n");
        return;
    }
    if(get(key) != 0){
        printf("duplicate key, put fail\n");
        return;
    }
    // basic way
    if(T[hash1(key)] == 0){
        T[hash1(key)] = key;
    }else if(T[hash2(key)] == 0){
        T[hash2(key)] = key;
    }else{
        // two place for one certain key has been occupied, need evict others
        // basic way
        KeyType evicted = key;
        // determine which pos hash1 or hash2 to put key
        // 0 is hash1, 1 is hash2
        int which = 0;
        // first evict key in hash1
        int idx = hash1(evicted);
        // != 0 means place has been occupied
        // if there is a cycle, maybe cannot terminate
        int count = 0;
        int pre_pos = -1;
        while(T[idx] != 0){
            printf("evicted key %d from %d to %d\n", evicted, pre_pos, idx);
            swap(&evicted, &T[idx]);
            pre_pos = idx;
            which = 1 - which;
            idx = (which == 0)?hash1(evicted):hash2(evicted);
            if(count > bound){
                printf("dead loop!\n");
                exit(-1);
            }
            ++count;
        }
        printf("evicted key %d from %d to %d\n", evicted, pre_pos, idx);
        T[idx] = evicted;
    }
}


//void Cuckoo::rehash() {
//    std::vector<KeyType> rehashArr;
//
//}
