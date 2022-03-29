#include "SSTable.h"

//从文件中load还是有问题啊，在search的时候无法正常进行查找

SSTableCache::SSTableCache(){}


SSTableCache::SSTableCache(std::string path, uint64_t &ts, std::string dir)
{
    std::ifstream file(path, std::ios::binary);
    if(!file){
        std::cout << "open file error!" << std::endl;
        exit(-1);
    }
    addr = dir;
    file.seekg(0,std::ios::beg);
    file.read(reinterpret_cast<char*>(&timeStamp), sizeof(uint64_t));
    ts = timeStamp;
    file.read(reinterpret_cast<char*>(&KVNum), sizeof(uint64_t));
    file.read(reinterpret_cast<char*>(&minKey), sizeof(uint64_t));
    file.read(reinterpret_cast<char*>(&maxKey), sizeof(uint64_t));
    char *bloomStr = new char[10240];
    // file.read(reinterpret_cast<char*>(bloom), 10240);
    file.read(bloomStr, 10240);
    bloom = new BloomFilter(bloomStr);
    uint64_t key;
    uint32_t offset;
    // long location = file.tellg();
    // if(timeStamp < 10)
    //     std::cout << "location: " << location << std::endl; 
    // char *remain = new char()
    for(uint64_t i = 0; i < KVNum; ++i){
        file.read(reinterpret_cast<char*>(&key), sizeof(uint64_t));
        file.read(reinterpret_cast<char*>(&offset), sizeof(uint32_t));
        if(i < 1000 && timeStamp == 1){
            // std::cout << offset << std::endl; 
            // std::cout << "offset: " << offset << std::endl; 
        }
        // if(i < 1000)
        //     std::cout << offset << std::endl; 
        indexTable.push_back(Index(key, offset));
    }
}

SSTableCache::SSTableCache(std::string &bufferStr, std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max){
    KVNum = num;
    timeStamp = tSt;
    minKey = min;
    maxKey = max;
    addr = path;
    bloom = new BloomFilter();
    uint32_t initBias = 10272 + 12 * KVNum;

    for(uint64_t i = 0; i < KVNum; ++i){
        indexTable.push_back(Index(li[i].first, initBias));
        initBias += (li[i].second).length();
        bufferStr += li[i].second;
        // std::cout << "insert" << std::endl;
        bloom->Insert(li[i].first);
        // std::cout << "single length: " << li[i].second.length() << std::endl;
    }
}

bool SSTableCache::Search(const uint64_t &key, uint32_t &offset, uint32_t &length, bool &isEnd)
{
    // std::cout << "search key: " << key << std::endl;
    if(bloom->Search(key) == false)
        return false;
    else{
        KVNum = indexTable.size();
        uint64_t left = 0, right = indexTable.size() - 1, mid;
        while(left <= right){
            mid = left + (right - left) / 2;
            if(indexTable[mid].key == key){
                offset = indexTable[mid].offset;
                //此处假设，如果是最后一个串的话，将其设为-1，实际上就是最大的无符号数了
                if(mid != KVNum - 1){
                    length = indexTable[mid + 1].offset - offset;
                    if(key == 5922){
                        std::cout << length <<" " << indexTable[mid + 1].offset << " " << offset << std::endl;
                    }
                }
                else{
                    isEnd = true;
                    //这里直接用INT_MAX解决，实际上少了一半的长度，可能会出问题，回头再看，目前找到new的参数是size_t，但是用
                    //uint32_t的最大值初始化时会报错
                    length = INT_MAX;
                }
                return true;
            }
            else if(indexTable[mid].key < key){
                left = mid + 1;
            }
            else{
                right = mid - 1;
            }
        } 
    }
    return false;
}

std::string SSTableCache::toFileName()
{
    return addr + "/f" + std::to_string(timeStamp) + "-" + std::to_string(KVNum) +"-" + std::to_string(minKey) + "-" + std::to_string(maxKey) + ".sst";
    // return addr;
}

void SSTableCache::Reset()
{
    delete bloom;
}

SSTableCache::~SSTableCache()
{
    delete bloom;
}


SSTable::SSTable()
{
    size = 0;
    count = 0;
}

SSTable::SSTable(SSTableCache *cache)
{
    addr = cache->getAddr();
    count = cache->getKVNum();
    //size是cache部分的长度
    size = 10272 + 12 * count;
    timeStamp = cache->timeStamp;
    std::string fileName = cache->toFileName();
    std::ifstream file(fileName, std::ios::binary);
    std::string val;
    uint32_t size;
    for(uint64_t i = 0; i < cache->KVNum - 1; ++i){
        size = cache->indexTable[i + 1].offset - cache->indexTable[i].offset;
        char *res = new char[size];
        file.seekg(cache->indexTable[i].offset, std::ios::beg);
        file.read(res, size);
        table.push_back(std::pair<uint64_t, std::string>(cache->indexTable[i].key, std::string(res)));
        delete res;
    }
    file.seekg(0,std::ios::end);
    int Readsize = file.tellg();
    size = Readsize - cache->indexTable[cache->KVNum - 1].offset;
    char *res = new char[size];
    file.seekg(cache->indexTable[cache->KVNum - 1].offset, std::ios::beg);
    file.read(res, size);
    table.push_back(std::pair<uint64_t, std::string>(cache->indexTable[cache->KVNum - 1].key, std::string(res)));
    delete res;
}

SSTableCache* SSTable::SaveSSTable(std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max)
{
    std::string bufferStr;
    SSTableCache *newCache = new SSTableCache(bufferStr, path, num, tSt, li, min, max);
    std::string fileName = path + "/f" + std::to_string(tSt) + "-" + std::to_string(num) +"-" + std::to_string(min) + "-" + std::to_string(max) + ".sst";
    addr = fileName;
    // std::cout << "fileName: " << fileName << std::endl;
    std::ofstream outFile(fileName, std::ios::out | std::ios::binary);
    outFile.write(reinterpret_cast<char*>(&(newCache->timeStamp)), sizeof(uint64_t));
    outFile.write(reinterpret_cast<char*>(&(newCache->KVNum)), sizeof(uint64_t));
    outFile.write(reinterpret_cast<char*>(&(newCache->minKey)), sizeof(uint64_t));
    outFile.write(reinterpret_cast<char*>(&(newCache->maxKey)), sizeof(uint64_t));
    char *buffer = new char[10240];
    buffer = newCache->bloom->writeTofile(buffer);
    // outFile.write(reinterpret_cast<char*>(newCache->bloom), 10240);
    outFile.write(buffer, 10240);
    int n = newCache->indexTable.size();
    //这里相当于遍历了两次，可不可以再优化？
    // long location = outFile.tellp();
    // if(newCache->timeStamp < 10)
    //     std::cout << "write location: " << location << std::endl;
    for(int i = 0; i < n; ++i){
        outFile.write(reinterpret_cast<char*>(&(newCache->indexTable[i].key)), sizeof(uint64_t));
        outFile.write(reinterpret_cast<char*>(&(newCache->indexTable[i].offset)), sizeof(uint32_t));
        // if(newCache->timeStamp == 1 && i < 1000)
        //     std::cout << newCache->indexTable[i].key << std::endl;
    }
    // std::cout <<"length: " << bufferStr.length() << std::endl;
    // size_t i = 0, len = bufferStr.length();
    outFile << bufferStr;
    // while(100000 * i < len)
    //     outFile.write(reinterpret_cast<char*>(&bufferStr), 100000 * i);
    outFile.close();
    // std::ifstream fin(fileName);
    // fin.seekg(0, std::ios::end);
	// size_t size = fin.tellg();
    // fin.close();
	// std::cout << "size: " << size << std::endl;
    return newCache;
}

void SSTable::SSTableToFile(SSTableCache *cache)
{
    std::string fileName = cache->toFileName();
    std::ofstream outFile(fileName, std::ios::out | std::ios::binary);
    outFile.write(reinterpret_cast<char*>(&(cache->timeStamp)), sizeof(uint64_t));
    outFile.write(reinterpret_cast<char*>(&(cache->KVNum)), sizeof(uint64_t));
    outFile.write(reinterpret_cast<char*>(&(cache->minKey)), sizeof(uint64_t));
    outFile.write(reinterpret_cast<char*>(&(cache->maxKey)), sizeof(uint64_t));
    char *buffer = new char[10240];
    buffer = cache->bloom->writeTofile(buffer);
    outFile.write(buffer, 10240);
    size_t size = cache->indexTable.size();
    for(size_t i = 0; i < size; ++i){
        outFile.write(reinterpret_cast<char*>(&(cache->indexTable[i].key)), sizeof(uint64_t));
        outFile.write(reinterpret_cast<char*>(&(cache->indexTable[i].offset)), sizeof(uint32_t));
    }
    for(size_t i = 0; i < size; ++i){
        outFile.write(table[i].second.c_str(), table[i].second.length());
    }
}

std::string SSTable::ReadSSTable(uint32_t offset, uint32_t length, std::string fileName, bool isEnd)
{
    std::ifstream file(fileName, std::ios::binary);
    char *res = new char[length];
    int Readsize;
    if(!file){
        std::cout<< "can't read the file!" << std::endl;
        exit(0);
    }
    if(!isEnd){
        file.seekg(offset, std::ios::beg);
        file.read(res, length);
        // std::cout << "result :" << res << std::endl;
        // result = res;
    }
    else{
        file.seekg(0,std::ios::end);
        Readsize = file.tellg();
        length = Readsize - offset;
        file.seekg(offset, std::ios::beg);
        // file.read(reinterpret_cast<char*>(&result), size);
        file.read(res, length);
        // file >> result;
    }
    std::string result(res);
    delete res;
    file.close();
    result = result.substr(0, length);
    // std::cout << result << std::endl;
    return result;
}


void SSTable::mergeSort(std::vector<SSTable> &Tlist)
{
    while(Tlist.size() > 1){
        SSTable table1 = Tlist.back();
        Tlist.pop_back();
        SSTable table2 = Tlist.back();
        Tlist.pop_back();
        SSTable newTable = merge(table1, table2);
        Tlist.push_back(newTable);
    }  
    std::cout << "after mergeSort: " << Tlist[0].table.size() << std::endl;
}

void SSTable::setAddr(std::string &address)
{
    addr = address;
}

std::vector<SSTableCache*> SSTable::division(SSTable &Table)
{
    std::vector<std::pair<uint64_t, std::string>> tab = Table.table;
    uint64_t min, max, num;
    std::vector<std::pair<uint64_t, std::string>> li;
    std::vector<SSTableCache*> result;
    int remainSpace = 2086880;
    size_t i = 0;
    size_t size = tab.size();
    // std::cout << "tab size: " << size << std::endl;
    while(i < size){
        // if(i == 0)
        //     std::cout << tab[i].first << std::endl;
        while(i < size && remainSpace >= 0){
            // if(tab[i].first < 4500)
            //     std::cout << i << " " << tab[i].first << std::endl;
            //这里莫名会多一个元素，加一个等于号试试？
            if(remainSpace - 12 - tab[i].second.length() <= 0)
                break;
            li.emplace_back(tab[i]);
            remainSpace = remainSpace - 12 - tab[i].second.length();
            ++i;
        }
        remainSpace = 2086880;
        min = li[0].first;
        max = li[li.size() - 1].first;
        num = li.size();
        std::cout << min << " " << max << " " << num << std::endl;
        //这里已经写入磁盘了
        SSTableCache *newcache = SaveSSTable(Table.addr, num, Table.timeStamp, li, min, max);
        result.emplace_back(newcache);
        li.clear();
    }
    return result;
}

SSTable SSTable::merge(const SSTable &table1, const SSTable &table2)
{
    // std::string addr1 = s1.toFileName();
    // std::string addr2 = s2.toFileName();
    // std::ifstream file1(addr1, std::ios::binary);
    // std::ifstream file2(addr2, std::ios::binary);
    // SSTable newTable;
    // SSTableCache newcache;
    // std::vector<Index> indtable1 = s1.indexTable, indtable2 = s2.indexTable;
    // uint64_t cap1 = s1.KVNum, cap2 = s2.KVNum, k = 0, i = 0, j = 0;
    // newcache.timeStamp = std::max(s1.timeStamp, s2.timeStamp);
    // while(i < cap1 && j < cap2){
    //     if(indtable1[i].key < indtable2[j].key){

    //     }
    // }
    // SSTable table1(*s1), table2(*s2);
    SSTable newTable;
    newTable.addr = std::max(table1.addr, table2.addr);
    // SSTableCache *newcache;
    // newcache->timeStamp = std::max((*s1)->timeStamp, (*s2)->timeStamp);
    uint64_t i = 0, j = 0;
    while(i < table1.count && j < table2.count){
        if(table1.table[i].first < table2.table[j].first){
            newTable.table.push_back(table1.table[i]);
            ++i;
        }
        else if(table1.table[i].first > table2.table[j].first){
            newTable.table.push_back(table2.table[j++]);
            ++j;
        }
        else{
            if(table1.timeStamp < table2.timeStamp)
                newTable.table.push_back(table2.table[j]);
            else
                newTable.table.push_back(table1.table[i]);
            ++i;
            ++j;
        }
    }
    while(i < table1.count){
        newTable.table.push_back(table1.table[i++]);
    }
    while(j < table2.count){
        newTable.table.push_back(table2.table[j++]);
    }
    newTable.timeStamp = std::max(table1.timeStamp, table2.timeStamp);
    newTable.count = newTable.table.size();
    std::cout << "after merge: " << newTable.table.size() << std::endl;
    return newTable;
}






