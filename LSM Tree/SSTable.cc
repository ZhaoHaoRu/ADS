#include "SSTable.h"

//从文件中load还是有问题啊，在search的时候无法正常进行查找

SSTableCache::SSTableCache(){}


SSTableCache::SSTableCache(std::string path, uint64_t &ts, std::string dir)
{
    std::ifstream file(path, std::ios::binary);
    // std::ifstream file(path);
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
    // std::shared_ptr<BloomFilter> bloom = std::make_shared<BloomFilter>();
    bloom = new BloomFilter();
    uint32_t initBias = 10272 + 12 * KVNum;
    buffer_size = initBias;
    std::ofstream file("bug_report.sst", std::ios::out|std::ios::app);

    for(uint64_t i = 0; i < KVNum; ++i){
        // if((li[i].first >= 9062 && li[i].first <= 9117) || (li[i].first >= 12529 && li[i].first <= 12568))
        //     file << "first insert: "<<li[i].first << " " <<initBias << std::endl;
        // if(li[i].first != li[i].second.length() - 1){
        //     std::cout << "write error! " << path << " " << "try to write: " << li[i].second<< std::endl;
        //     std::cout << li[i].first  << " " << li[i].second.length()<< std::endl; 
        //     exit(0);
        // }
        // if(li[i].first == 0){
        //     std::cout << "first at 59: " << li[i].second << std::endl; 
        // }
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
        // if(key == 0)
        //     std::cout << this->toFileName() << std::endl;
        return false;
    }
    else{
        // std::cout << this->toFileName() << std::endl;
        KVNum = indexTable.size();
        uint64_t left = 0, right = indexTable.size() - 1, mid;
        while(left <= right){
            mid = left + (right - left) / 2;
            if(mid < 0 || mid > KVNum - 1){
                // std::cout << key << " " << mid << " boundary error!" << std::endl;
                return false;
            }
            if(indexTable[mid].key == key){
                offset = indexTable[mid].offset;
                //此处假设，如果是最后一个串的话，将其设为-1，实际上就是最大的无符号数了
                if(mid != KVNum - 1){
                    length = indexTable[mid + 1].offset - offset;
                }
                else{
                    isEnd = true;
                    //这里直接用INT_MAX解决，实际上少了一半的长度，可能会出问题，回头再看，目前找到new的参数是size_t，但是用
                    //uint32_t的最大值初始化时会报错
                    length = INT_MAX;
                }
                // if(mid != KVNum - 1 && length != (key + 1)){
                //     std::cout << "error key: " << key << std::endl;
                //     std::cout << "offset: "<< indexTable[mid].offset << std::endl;
                //     std::cout << "next offset: "<<indexTable[mid + 1].offset << std::endl;
                // }
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
        std::cout << "line 165 offset error!" << std::endl;
    }
    // std::cout << "file size: " << size << std::endl;
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
            // if(cache->indexTable[i].key == 0){
            //     std::cout << "first at 188: " << std::string(res) << std::endl;
            // }
            table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[i].key, std::string(res)));
            // if(table.back().first != table.back().second.length() - 1){
            //     std::cout << "init size: " <<  size_tmp << std::endl;
            //     std::cout << "load string: " << res << std::endl;
            //     std::cout << "offset: " << cache->indexTable[i].offset << std::endl;
            //     std::cout << "error when load SSTable: " << fileName << " " << table.back().first << " " << table.back().second.length()<< std::endl;
            //     std::cout << table.back().second.length() << std::endl;
            //     if(table.back().second.length() == 6){
            //         exit(0);
            //     }
            // }
            delete []res;
        }
        catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug at line 153" << std::endl;
            std::cout << "alloc size: " << size_tmp << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
		}
    }
    std::string endStr;
    file >> endStr;
    table.emplace_back(std::pair<uint64_t, std::string>(cache->indexTable[cache->KVNum - 1].key,endStr));
    // if(table.back().first != table.back().second.length() - 1){
    //             std::cout << "error when load SSTable: " << fileName << " " << table.back().first << std::endl;
    //             std::cout << table.back().second.length() << std::endl;
    // }
}

SSTableCache* SSTable::SaveSSTable(std::string path, uint64_t num, uint32_t tSt, std::vector<std::pair<uint64_t, std::string>> &li, uint64_t min, uint64_t max)
{
    std::string bufferStr;
    uint64_t buffer_size = 0;

    SSTableCache *newCache = new SSTableCache(buffer_size, bufferStr, path, num, tSt, li, min, max);
    // std::shared_ptr<SSTableCache> newCache(new SSTableCache(bufferStr, path, num, tSt, li, min, max));
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
        // if(newCache->indexTable[i].key == 0){
        //     std::cout << "first at 238: " << li[i].second << std::endl;
        // }
        *(uint64_t*)index = newCache->indexTable[i].key;
        index += 8;
        *(uint32_t*)index = newCache->indexTable[i].offset;
        index += 4;
        if(newCache->indexTable[i].offset + li[i].second.length() > buffer_size){
            std::cout << "buffer overflow!" << std::endl;
            exit(-1);
        }
        if(li[i].second.length() >= size_t(~0)){
            std::cout << "line 265 bad alloc!" << std::endl;
            exit(-1);
        }
        memcpy(buffer + newCache->indexTable[i].offset, li[i].second.c_str(), li[i].second.length());
    }
    outFile.write(buffer, buffer_size);
    // std::cout << newCache->toFileName() << " write into file: " << buffer_size << std::endl;
    delete[] buffer;
    delete[] bitbuffer;
    outFile.close();
    return newCache;
    // std::ofstream outFile(fileName, std::ios::out);
    // outFile.write(reinterpret_cast<char*>(&(newCache->timeStamp)), sizeof(uint64_t));
    // outFile.write(reinterpret_cast<char*>(&(newCache->KVNum)), sizeof(uint64_t));
    // outFile.write(reinterpret_cast<char*>(&(newCache->minKey)), sizeof(uint64_t));
    // outFile.write(reinterpret_cast<char*>(&(newCache->maxKey)), sizeof(uint64_t));
    // char *buffer = new char[10240];
    // buffer = newCache->bloom->writeTofile(buffer);
    // outFile.write(buffer, 10240);
    // outFile.close();
    // std::ifstream fin(fileName);
    // fin.seekg( 0, std::ios::end );
    // int size = fin.tellg();
	// fin.close();
	// if(size != 10272){
    //     std::cout << "header write error!" << std::endl;
    //     std::cout << size << std::endl;
    //     exit(0);
    // }
    // outFile.open(fileName, std::ios::out | std::ios::app);
    // int n = newCache->indexTable.size();
    // //这里相当于遍历了两次，可不可以再优化？
    // if(n != newCache->KVNum){
    //     std::cout <<  "count error !" << std::endl;
    // }
    // for(int i = 0; i < n; ++i){
    //     outFile.write(reinterpret_cast<char*>(&(newCache->indexTable[i].key)), sizeof(uint64_t));
    //     outFile.write(reinterpret_cast<char*>(&(newCache->indexTable[i].offset)), sizeof(uint32_t));
    //     if(i < n - 1 && (newCache->indexTable[i + 1].offset - newCache->indexTable[i].offset) != newCache->indexTable[i].key + 1)
    //         std::cout << "save error: " << newCache->indexTable[i].key << " " << newCache->indexTable[i + 1].offset - newCache->indexTable[i].offset<< " " << newCache->toFileName() << std::endl;
    // }
    // outFile.close();
    // std::ifstream file2(fileName);
    // file2.seekg(0, std::ios::end);
    // int size1 = file2.tellg();
	// file2.close();
	// if(size1 != 10272 + newCache->KVNum * 12){
    //     std::cout << "header write error after write index!" << std::endl;
    //     std::cout << size1  << " " << newCache->KVNum * 12 << std::endl;
    //     exit(0);
    // }
    //  outFile.open(fileName, std::ios::out | std::ios::app);
    // for(int i = 0; i < n; ++i){
    //     outFile.write(li[i].second.c_str(), li[i].second.length());
    //     if(li[i].second.length() != newCache->indexTable[i].key + 1){
    //         std::cout << "write string error: " << li[i].second.length() << " " << newCache->indexTable[i].key << std::endl;
    //     }
    // }
    // uint32_t length = newCache->indexTable[n - 1].offset - newCache->indexTable[0].offset;
    // // if(bufferStr.length() != length){
    // //     std::cout << "save error: " << bufferStr.length() << " " << length << " " << newCache->toFileName() << std::endl;
    // // }
    // // outFile << bufferStr;
    // outFile.close();
    // std::ifstream file(fileName);
    // file.seekg( 0, std::ios::end );
    // int size2 = file.tellg();
    // std::cout << newCache->toFileName() << " " << size2 << std::endl;
	// file.close();
    // return newCache;
}

void SSTable::SSTableToFile(SSTableCache *cache)
{
    std::string fileName = cache->toFileName();
    std::ofstream outFile(fileName, std::ios::out | std::ios::binary);
    // std::ofstream outFile(fileName, std::ios::out);
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
    // std::ifstream file(fileName);
    if(length >= 2086880 && !isEnd){
        std::cout << "length: " << length << std::endl;
        std::cout << "line 371 bad alloc!" << std::endl;
        exit(-1);
    }
    //Address 0x4f180f8 is 5 bytes after a block of size 3 alloc'd
    char *res = nullptr;
    if(isEnd){
        if(2097152 < offset){
             std::cout << "line 383 bad alloc!" << std::endl;
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
        //这里回头要替换嘛？
        file.seekg(0,std::ios::end);
        Readsize = file.tellg();
        length = Readsize - offset;
        file.seekg(offset, std::ios::beg);
        // file.read(res, length);
        std::string result;
        file >> result;
        return result.substr(0, length);
    }
    //invalid read of size of XX
    
}

//先用correct code把SSTable中的merge、save、division改了
void SSTable::mergeSort(std::vector<SSTable*> &Tlist)
{
    // uint32_t size = Tlist.size();
    // std::cout << "merge size: " << size << std::endl;
    // if(size <= 1)
    //     return;
    // uint32_t mid = size / 2;
    // std::vector<SSTable*> next;
    // for(uint32_t i = 0; i < mid; ++i){
    //     std::cout << "merge index: " <<  i * 2 << " " << i * 2 + 1 << std::endl;
    //     next.emplace_back(merge(Tlist[i * 2], Tlist[i * 2 + 1]));
    // }
    // if(size % 2 != 0)
    //     next.emplace_back(Tlist[size - 1]);
    // mergeSort(next);
    // Tlist = next;
    while(Tlist.size() > 1){
        SSTable* table1 = Tlist.back();
        Tlist.pop_back();
        SSTable* table2 = Tlist.back();
        Tlist.pop_back();
        SSTable* newTable = merge(table1, table2);
        Tlist.emplace_back(newTable);
        // delete table1;
        // delete table2;
    }  
    // std::cout << "after mergeSort: " << Tlist[0]->table.size() << std::endl;
}

void SSTable::setAddr(std::string &address)
{
    addr = address;
}

std::vector<SSTableCache*> SSTable::division(SSTable *Table)
{
    // std::cout << "begin division" << std::endl;
    std::list<std::pair<uint64_t, std::string>> tab = Table->table;
    uint64_t min, max, num;
    //下一步如果不行的话，把这个vector也改掉
    std::vector<std::pair<uint64_t, std::string>> li;
    std::vector<SSTableCache*> result;
    int remainSpace = 2086880;
    auto it = tab.begin();
    while(it != tab.end()){
        while(it != tab.end() && remainSpace > 0){
            //这里莫名会多一个元素，加一个等于号试试？
            // if((*it).first == 0){
            //     std::cout << "first at 429: " << (*it).second << std::endl;
            // }
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
    // SSTableCache *newcache;
    // newcache->timeStamp = std::max((*s1)->timeStamp, (*s2)->timeStamp);
    auto i = (table1->table).begin(), j = (table2->table).begin();
    // uint64_t i = 0, j = 0;
    // i = table1->table.begin();
    // j = table2->table.begin();
    // std::cout << "get line: 435" << std::endl;
    // std::cout << table1->table.size() << ' ' << table2->table.size() << std::endl;
    int k = 0, m = 0, n = 0;
    // std::cout << table1->addr << " " << table2->addr << std::endl;
    while(i != table1->table.end() && j != table2->table.end()){
        // std::cout << "index: " << k << std::endl;
        // std::cout << "key1: " << (*i).first << std::endl;
        // std::cout << "key2: " << (*j).first << std::endl;
        k++;
        // if((*i).first == 0){
        //     std::cout << "first at 470: " << (*i).second << std::endl;
        // }
        // if((*j).first == 0){
        //     std::cout << "first at 473: " << (*j).second << std::endl;
        // }
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
    // std::cout << "get line: 454" << std::endl;
    while(i != table1->table.end()){
        // std::cout << "index i : " << k << std::endl;
        // std::cout << (*i).first << std::endl;
        k++;
        newTable->table.emplace_back(*i);
        ++i;
        ++m;
    }
    while(j != table2->table.end()){
        // std::cout << "index j : " << k << std::endl;
        // std::cout << (*j).first << std::endl;
        k++;
        newTable->table.emplace_back(*j);
        ++j;
        ++n;
    }
    // std::cout << "get line: 463" << std::endl;
    // std::cout << m << " " << n << " " << k << std::endl;
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




