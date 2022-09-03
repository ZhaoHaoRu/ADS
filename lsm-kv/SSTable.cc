#include "SSTable.h"


SSTableCache::SSTableCache(){}


SSTableCache::SSTableCache(std::string path, uint64_t &ts, std::string dir)
{
    std::ifstream file(path, std::ios::binary);
    indexTable.reserve(0);
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
    char *bloomStr = new char[10241];
    bloomStr[10240] = '\0';
    file.read(bloomStr, 10240);
    bloom = new BloomFilter(bloomStr);
    uint64_t key;
    uint32_t offset;
    for(uint64_t i = 0; i < KVNum; ++i){
        file.read(reinterpret_cast<char*>(&key), sizeof(uint64_t));
        file.read(reinterpret_cast<char*>(&offset), sizeof(uint32_t));
        indexTable.emplace_back(Index(key, offset));
    }
    delete []bloomStr;
}

SSTableCache::SSTableCache(uint64_t &buffer_size, std::string &bufferStr, std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max){
    KVNum = num;
    timeStamp = tSt;
    minKey = min;
    maxKey = max;
    addr = path;
    bloom = new BloomFilter();
    uint32_t initBias = 10272 + 12 * KVNum;
    buffer_size = initBias;
    std::ofstream file("bug_report.sst", std::ios::out|std::ios::app);

    for(uint64_t i = 0; i < KVNum; ++i){
        indexTable.emplace_back(Index(li[i].first, initBias));
        initBias += (li[i].second).length();
        bufferStr += li[i].second;
        buffer_size += li[i].second.length();
        bloom->Insert(li[i].first);
    }
    file.close();
}

bool SSTableCache::Search(const uint64_t &key, uint32_t &offset, uint32_t &length, bool &isEnd, uint64_t &pos)
{
    if(bloom->Search(key) == false){
        return false;
    }
    else{
        KVNum = indexTable.size();
        uint64_t left = 0, right = indexTable.size() - 1, mid;
        while(left <= right){
            mid = left + (right - left) / 2;
            if(mid < 0 || mid > KVNum - 1){
                return false;
            }
            if(indexTable[mid].key == key){
                offset = indexTable[mid].offset;
                if(mid != KVNum - 1){
                    length = indexTable[mid + 1].offset - offset;
                }
                else{
                    isEnd = true;
                    //这里直接用INT_MAX解决，实际上少了一半的长度，可能会出问题，回头再看，目前找到new的参数是size_t，但是用
                    //uint32_t的最大值初始化时会报错
                    length = INT_MAX;
                }
                pos = mid;
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

bool SSTableCache::Scan(uint64_t &key, uint64_t &pos, const uint64_t &key1, const uint64_t &key2, uint32_t &offset, uint32_t &length, bool &isEnd)
{
    if(minKey < key1 || maxKey > key2)
        return false;
    for(key = key1; key <= key2; ++key){
        bool result = Search(key, offset, length, isEnd, pos);
        if(result){
            return true;
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
    // std::ifstream file(fileName);
    if(!file) {
        printf("Fail to open file %s", fileName);
        exit(-1);
    }
    std::string val;
    int size_tmp;
    file.seekg(0, std::ios::end);
    int size = file.tellg();
    if(10272 + count * 12 != cache->indexTable[0].offset){
        std::cout << "line 173 offset error!" << std::endl;
    }

    file.seekg(cache->indexTable[0].offset, std::ios::beg);
    for(uint64_t i = 0; i < cache->KVNum - 1; ++i){
        size_tmp = cache->indexTable[i + 1].offset - cache->indexTable[i].offset;
        char *res = nullptr;
        try{
            if(cache->indexTable[i].offset > size){
                std::cout<< "file to small! " << size << " " << cache->indexTable[i].offset << std::endl;
                exit(0);
            }
            file.seekg(cache->indexTable[i].offset, std::ios::beg);
            if(size_tmp < 0){
                std::cout << "line 187 bad alloc!" << std::endl;
                exit(-1);
            }
            res = new char[size_tmp + 1];
            res[size_tmp] = '\0';
            file.read(res, size_tmp);
            table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[i].key, std::string(res)));
            delete []res;
        }
        catch(std::bad_alloc &ba){
			std::cout << "compaction two level error at line 193" << std::endl;
            std::cout << "alloc size: " << size_tmp << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
		}
    }
    std::string endStr;
    file >> endStr;
    table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[cache->KVNum - 1].key,endStr));
}

SSTableCache* SSTable::SaveSSTable(std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max)
{
    std::string bufferStr;
    uint64_t buffer_size = 0;

    SSTableCache *newCache = new SSTableCache(buffer_size, bufferStr, path, num, tSt, li, min, max);
    std::string fileName = path + "/f" + std::to_string(tSt) + "-" + std::to_string(num) +"-" + std::to_string(min) + "-" + std::to_string(max) + ".sst";
    addr = fileName;
    std::ofstream outFile(fileName, std::ios::out | std::ios::binary);
    if(buffer_size >= uint64_t(~0)){
        std::cout << " line 236 bad alloc!" << std::endl;
        exit(-1);
    }
    char *buffer = new char[buffer_size];

    *(uint64_t*)buffer = tSt;
    *(uint64_t*)(buffer + 8) = newCache->KVNum;
    *(uint64_t*)(buffer + 16) = newCache->minKey;
    *(uint64_t*)(buffer + 24) = newCache->maxKey;

    char *bitbuffer = new char[10240];
    bitbuffer = newCache->bloom->writeTofile(bitbuffer);
    memcpy(buffer + 32, bitbuffer, 10240);

    char *index = buffer + 10272;
    int n = newCache->indexTable.size();
    for(int i = 0; i < n; ++i){
        *(uint64_t*)index = newCache->indexTable[i].key;
        index += 8;
        *(uint32_t*)index = newCache->indexTable[i].offset;
        index += 4;
        if(newCache->indexTable[i].offset + li[i].second.length() > buffer_size){
            std::cout << "buffer overflow!" << std::endl;
            exit(-1);
        }
        if(li[i].second.length() >= size_t(~0)){
            std::cout << "line 248 bad alloc!" << std::endl;
            exit(-1);
        }
        memcpy(buffer + newCache->indexTable[i].offset, li[i].second.c_str(), li[i].second.length());
    }
    outFile.write(buffer, buffer_size);
    delete[] buffer;
    delete[] bitbuffer;
    outFile.close();
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
    std::list<std::pair<uint64_t, std::string>>::iterator it;
	for (it = table.begin(); it!=table.end(); it++){
        outFile.write((*it).second.c_str(), (*it).second.length());
    }
    delete []buffer;
}

size_t SSTable::getEndlength()
{
    auto it = table.back();
    if(it.second.length() == 0)
        std::cout << "getendlength error key: "<< it.first << std::endl;
    return it.second.length();
}

std::string SSTable::ReadSSTable(uint32_t offset, uint32_t length, std::string fileName, bool isEnd)
{
    std::ifstream file(fileName, std::ios::binary);
    if(length >= 2086880 && !isEnd){
        std::cout << "length: " << length << std::endl;
        std::cout << "line 299 bad alloc!" << std::endl;
        exit(-1);
    }

    char *res = nullptr;
    if(isEnd){
        if(2097152 < offset){
             std::cout << "line 306 bad alloc!" << std::endl;
             exit(-1);
        }
        length = 2097152 - offset;
    }
    res = new char[length + 1];
    if(res == nullptr){
        std::cout << "bad alloc at SSTable.cc line 382!" << std::endl;
        exit(-1);
    }
    int Readsize;
    if(!file){
        std::cout << fileName << std::endl;
        std::cout<< "can't read the file!" << std::endl;
        exit(0);
    }
    if(!isEnd){
        file.seekg(offset, std::ios::beg);
        file.read(res, length);
        std::string result(res);
        delete []res;
        file.close();
        result = result.substr(0, length);
        return result;
    }
    else{
        file.seekg(0,std::ios::end);
        Readsize = file.tellg();
        length = Readsize - offset;
        file.seekg(offset, std::ios::beg);
        std::string result;
        file >> result;
        return result.substr(0, length);
    }
    
}


void SSTable::mergeSort(std::vector<SSTable*> &Tlist)
{
    while(Tlist.size() > 1){
        SSTable* table1 = Tlist.back();
        Tlist.pop_back();
        SSTable* table2 = Tlist.back();
        Tlist.pop_back();
        SSTable* newTable = merge(table1, table2);
        Tlist.emplace_back(newTable);
    }  
}

void SSTable::setAddr(std::string &address)
{
    addr = address;
}

std::vector<SSTableCache*> SSTable::division(SSTable *Table)
{
    std::list<std::pair<uint64_t, std::string>> tab = Table->table;
    uint64_t min, max, num;
    std::vector<std::pair<uint64_t, std::string>> li;
    std::vector<SSTableCache*> result;
    int remainSpace = 2086880;
    auto it = tab.begin();
    while(it != tab.end()){
        while(it != tab.end() && remainSpace > 0){
            if(remainSpace <= 12 + (*it).second.length())
                break;
            li.emplace_back(*it);
            remainSpace = remainSpace - 12 - (*it).second.length();
            ++it;
        }
        remainSpace = 2086880;
        min = li[0].first;
        max = li[li.size() - 1].first;
        num = li.size();
        //这里已经写入磁盘了
        SSTableCache *newcache = SaveSSTable(Table->addr, num, Table->timeStamp, li, min, max);
        result.emplace_back(newcache);
        li.clear();
    }
    std::vector<std::pair<uint64_t, std::string>>().swap(li);
    return result;
}

SSTable* SSTable::merge(const SSTable *table1, const SSTable *table2)
{
    SSTable *newTable = new SSTable();
    newTable->addr = std::max(table1->addr, table2->addr);
    auto i = (table1->table).begin(), j = (table2->table).begin();
    int k = 0, m = 0, n = 0;
    while(i != table1->table.end() && j != table2->table.end()){
        k++;
        if((*i).first < (*j).first){
            newTable->table.emplace_back(*i);
            ++i;
            ++m;
        }
        else if((*i).first > (*j).first){
            newTable->table.emplace_back(*j);
            ++j;
            ++n;
        }
        else{
            if(table1->timeStamp <= table2->timeStamp)
                newTable->table.emplace_back(*j); //其中table2是绝对没有归并过的，所以时间戳新的话是真的新
            else
                newTable->table.emplace_back(*i);
            ++i;
            ++j;
            ++m;
            ++n;
        }
    }
    while(i != table1->table.end()){
        k++;
        newTable->table.emplace_back(*i);
        ++i;
        ++m;
    }
    while(j != table2->table.end()){
        k++;
        newTable->table.emplace_back(*j);
        ++j;
        ++n;
    }
    newTable->timeStamp = std::max(table1->timeStamp, table2->timeStamp);
    newTable->count = newTable->table.size();
    delete table1;
    delete table2;
    return newTable;
}



SSTable::~SSTable()
{
    // std::vector<std::pair<size_t, std::string>>().swap(table);
}




