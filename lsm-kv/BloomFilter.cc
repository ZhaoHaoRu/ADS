#include "BloomFilter.h"
#include "MurmurHash3.h"
#include <string>
#include <string.h>

BloomFilter::BloomFilter(){}

BloomFilter::BloomFilter(const char *buffer){
    memcpy((char*)&bits, buffer, FilterSize/8);
}

void BloomFilter::Insert(const uint64_t &key){
    uint32_t insertKey[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, insertKey);
    for(int i = 0; i < 4; ++i){
        if(insertKey[i] >= FilterSize)
            insertKey[i] = insertKey[i] % FilterSize;
        bits.set(insertKey[i]);
    }
}

bool BloomFilter::Search(const uint64_t &key){
    uint32_t insertKey[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, insertKey);
    for(int i = 0; i < 4; ++i){
        if(insertKey[i] >= FilterSize)
            insertKey[i] = insertKey[i] % FilterSize;
        if(bits[insertKey[i]] == false)
            return false;
    }
    return true;
}

char* BloomFilter::writeTofile(char *buffer)
{
    memcpy(buffer, (char*)&bits, FilterSize/8);
    return buffer;
}