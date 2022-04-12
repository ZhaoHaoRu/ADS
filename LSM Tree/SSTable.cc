#include "SSTable.h"

//从文件中load还是有问题啊，在search的时候无法正常进行查找

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
        // if(i < 1000 && timeStamp == 1){
        //     // std::cout << offset << std::endl; 
        //     // std::cout << "offset: " << offset << std::endl; 
        // }
        // if(i < 1000)
        //     std::cout << offset << std::endl; 
        indexTable.emplace_back(Index(key, offset));
    }
    delete []bloomStr;
}

SSTableCache::SSTableCache(std::string &bufferStr, std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max){
    KVNum = num;
    timeStamp = tSt;
    minKey = min;
    maxKey = max;
    addr = path;
    // std::shared_ptr<BloomFilter> bloom = std::make_shared<BloomFilter>();
    bloom = new BloomFilter();
    uint32_t initBias = 10272 + 12 * KVNum;

    for(uint64_t i = 0; i < KVNum; ++i){
        indexTable.emplace_back(Index(li[i].first, initBias));
        initBias += (li[i].second).length();
        bufferStr += li[i].second;
        // std::cout << "insert" << std::endl;
        bloom->Insert(li[i].first);
        // std::cout << "single length: " << li[i].second.length() << std::endl;
    }
}

bool SSTableCache::Search(const uint64_t &key, uint32_t &offset, uint32_t &length, bool &isEnd, uint64_t &pos)
{
    // std::cout << "search key: " << key << std::endl;
    if(bloom->Search(key) == false){
        // std::cout << "bloom not found!" << std::endl;
        return false;
    }
    else{
        KVNum = indexTable.size();
        uint64_t left = 0, right = indexTable.size() - 1, mid;
        while(left <= right){
            mid = left + (right - left) / 2;
            if(mid < 0 || mid > KVNum - 1){
                std::cout << mid << " boundary error!" << std::endl;
                return false;
            }
            if(indexTable[mid].key == key){
                offset = indexTable[mid].offset;
                //此处假设，如果是最后一个串的话，将其设为-1，实际上就是最大的无符号数了
                if(mid != KVNum - 1){
                    length = indexTable[mid + 1].offset - offset;
                    // if(key == 5922){
                    //     std::cout << length <<" " << indexTable[mid + 1].offset << " " << offset << std::endl;
                    // }
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
            // std::cout << "key: " << key << std::endl;
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
    // int k = 129;
    addr = cache->getAddr();
    count = cache->getKVNum();
    //size是cache部分的长度
    size = 10272 + 12 * count;
    timeStamp = cache->timeStamp;
    std::string fileName = cache->toFileName();
    std::ifstream file(fileName, std::ios::binary);
    std::string val;
    uint32_t size_tmp;
    file.seekg(cache->indexTable[0].offset, std::ios::beg);
    for(uint64_t i = 0; i < cache->KVNum - 1; ++i){
        size_tmp = cache->indexTable[i + 1].offset - cache->indexTable[i].offset;
        char *res = nullptr;
        try{
            res = new char[size_tmp + 1];
        // std::cout << "line 145 size: " << size << std::endl;
            res[size_tmp] = '\0';
            file.read(res, size_tmp);
            table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[i].key, std::string(res)));
            delete []res;
        }
        catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug at line 153" << std::endl;
            std::cout << "alloc size: " << size_tmp << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
		}
        // file.seekg(cache->indexTable[i].offset, std::ios::beg);
    }
    // k = 147;
    // file.seekg(0,std::ios::end);
    // int Readsize = file.tellg();
    // size_tmp = Readsize - cache->indexTable[cache->KVNum - 1].offset;
    // // std::cout << "SSTable init size at end: " << cache->indexTable[cache->KVNum - 1].key << " " << size << std::endl;
    // char *res = nullptr;
    // try{
    //     res = new char[size_tmp + 1];
    //     std::cout << "line 158 size: " << size_tmp + 1 << std::endl;
    //     res[size_tmp] = '\0';
    //     file.seekg(cache->indexTable[cache->KVNum - 1].offset, std::ios::beg);
    //     file.read(res, size_tmp);
    //     table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[cache->KVNum - 1].key, std::string(res)));
    //     delete []res;
    // }
    // catch(std::bad_alloc &ba){
	// 		std::cout << "compaction two level bug at line 181" << std::endl;
    //         std::cout << size_tmp <<" " << Readsize << " " << cache->indexTable[cache->KVNum - 1].offset << std::endl;
    //         std::cout << fileName << std::endl;
	// 		std::cout << ba.what() << std::endl;
	// 		exit(0);
	// }
    std::string endStr;
    file >> endStr;
    // std::cout << "endStr: " << endStr << std::endl;
    table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[cache->KVNum - 1].key,endStr));
    // k = 155;
    // std::cout << "init SSTable: " << k << std::endl;
    
}

SSTableCache* SSTable::SaveSSTable(std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max)
{
    std::string bufferStr;

    SSTableCache *newCache = new SSTableCache(bufferStr, path, num, tSt, li, min, max);
    // std::shared_ptr<SSTableCache> newCache(new SSTableCache(bufferStr, path, num, tSt, li, min, max));
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
    // std::shared_ptr<char*> buffer(new char[10240]);
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
    // for(size_t i = 0; i < size; ++i){
    //     outFile.write(table[i].second.c_str(), table[i].second.length());
    //     std::cout << "SSTableToFile: " <<table[i].second.length() << std::endl;
    // }
    delete []buffer;
}

size_t SSTable::getEndlength()
{
    auto it = table.back();
    return it.second.length();
}

std::string SSTable::ReadSSTable(uint32_t offset, uint32_t length, std::string fileName, bool isEnd)
{
    std::ifstream file(fileName, std::ios::binary);
    char *res = new char[length];
    // std::cout << "line 249 size: " << length << std::endl;
    int Readsize;
    if(!file){
        std::cout << fileName << std::endl;
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
        // std::cout << "end length: " << length << std::endl;
        file.seekg(offset, std::ios::beg);
        // file.read(reinterpret_cast<char*>(&result), size);
        file.read(res, length);
        // file >> result;
    }
    std::string result(res);
    delete []res;
    file.close();
    result = result.substr(0, length);
    // std::cout << result << std::endl;
    return result;
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
        delete table1;
        delete table2;
    }  
    std::cout << "after mergeSort: " << Tlist[0]->table.size() << std::endl;
}

void SSTable::setAddr(std::string &address)
{
    addr = address;
}

std::vector<SSTableCache*> SSTable::division(SSTable *Table)
{
    std::list<std::pair<uint64_t, std::string>> tab = Table->table;
    uint64_t min, max, num;
    //下一步如果不行的话，把这个vector也改掉
    std::vector<std::pair<uint64_t, std::string>> li;
    std::vector<SSTableCache*> result;
    int remainSpace = 2086880;
    // size_t i = 0;
    // size_t size = tab.size();
    auto it = tab.begin();
    // std::cout << "tab size: " << size << std::endl;
    while(it != tab.end()){
        // if(i == 0)
        //     std::cout << tab[i].first << std::endl;
        while(it != tab.end() && remainSpace > 0){
            // if(tab[i].first < 4500)
            //     std::cout << i << " " << tab[i].first << std::endl;
            //这里莫名会多一个元素，加一个等于号试试？
            if(remainSpace - 12 - (*it).second.length() <= 0)
                break;
            li.emplace_back(*it);
            remainSpace = remainSpace - 12 - (*it).second.length();
            ++it;
        }
        remainSpace = 2086880;
        min = li[0].first;
        max = li[li.size() - 1].first;
        num = li.size();
        // std::cout << "end length before save: " << li[li.size() - 1].second.length() << std::endl;
        // std::cout << min << " " << max << " " << num << std::endl;
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
    // SSTableCache *newcache;
    // newcache->timeStamp = std::max((*s1)->timeStamp, (*s2)->timeStamp);
    auto i = (table1->table).begin(), j = (table2->table).begin();
    // uint64_t i = 0, j = 0;
    // i = table1->table.begin();
    // j = table2->table.begin();
    while(i != table1->table.end() && j != table2->table.end()){
        if((*i).first < (*j).first){
            newTable->table.emplace_back(*i);
            ++i;
        }
        else if((*i).first > (*j).first){
            newTable->table.emplace_back(*j);
            ++j;
        }
        else{
            if((*i).first == 0){
                std::cout << "come here !" << std::endl;
                std::cout << "timeStamp: " << table1->timeStamp << " " << table2->timeStamp << std::endl;
            }
            if(table1->timeStamp <= table2->timeStamp)
                newTable->table.emplace_back(*j); //其中table2是绝对没有归并过的，所以时间戳新的话是真的新
            else
                newTable->table.emplace_back(*i);
            ++i;
            ++j;
        }
    }
    while(i != table1->table.end()){
        newTable->table.emplace_back(*i);
        ++i;
    }
    while(j != table2->table.end()){
        newTable->table.emplace_back(*j);
        ++j;
    }
    newTable->timeStamp = std::max(table1->timeStamp, table2->timeStamp);
    // std::cout << "before merge: " << table1->table.size() << " " << table2->table.size() << std::endl;
    newTable->count = newTable->table.size();
    // std::cout << "after merge: " << newTable->table.size() << std::endl;
    return newTable;
}



SSTable::~SSTable()
{
    // std::vector<std::pair<size_t, std::string>>().swap(table);
}




