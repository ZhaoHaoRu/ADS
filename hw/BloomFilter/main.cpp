#include <iostream>
#include <bitset>
#include <iostream>
#include <cstdint>
#include <string>
#include <functional>
#include <stdint.h>

using namespace std;

const unsigned int m = 500;
const unsigned int k = 5;

class BloomFilter{
private:
    std::bitset<m> bits;
    int falseTime;

public:
    BloomFilter();
    void Prepare();
    void Insert(const int &key);
    bool Search(const int &key);
    void Test();
};

BloomFilter::BloomFilter():falseTime(0){}




size_t hash_f(int key)
{
    key = ~key + (key << 15); // key = (key << 15) - key - 1;
    key = key ^ (key >> 12);
    key = key + (key << 2);
    key = key ^ (key >> 4);
    key = key * 2057; // key = (key + (key << 3)) + (key << 11);
    key = key ^ (key >> 16);
    return key;
}


void BloomFilter::Prepare()
{
    for(int i = 0;i < 100; ++i){
        Insert(i);
    }
}

void BloomFilter::Insert(const int &key){
    for(int i = 0; i < k; ++i){
        size_t h_ret = hash_f(key * (i + 1));
        h_ret = h_ret % m;
        // cout << "key: " << key << endl;
        // cout << h_ret << endl;
        bits.set(h_ret, true);
    }
}

bool BloomFilter::Search(const int &key){
    for(int i = 0; i < k; ++i){
        size_t h_ret = hash_f(key + i);
        h_ret = h_ret % m;
        if(bits[h_ret] == false){
            return false;
        }
    }
    return true;
}

void BloomFilter::Test()
{
    for(int i = 100; i < 199; ++i){
        if(Search(i)){
            ++falseTime;
        }
    }
    double false_rate = double(falseTime) / 100;
    cout << false_rate << endl;
}

main(){
    BloomFilter bloom;
    bloom.Prepare();
    bloom.Test();
    return 0;
}