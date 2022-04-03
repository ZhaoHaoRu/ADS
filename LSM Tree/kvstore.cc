#include "kvstore.h"
#include <string>
#include <algorithm>
#include <list>
#include "utils.h"

// using namespace utils;
// using namespace std;

struct 
{
	bool operator()(const std::string &str1, const std::string &str2){
		int pos = str1.find('f');
		int num1 = stoi(str1.substr(pos + 1));
		pos = str2.find('f');
		int num2 = stoi(str2.substr(pos + 1));
		// std::cout << num1 << " " << num2 << std::endl;
		return num1 > num2;
	}
}Comp;

// struct 
// {
// 	bool operator()(const std::pair<size_t, uint64_t> &p1, const std::pair<size_t, uint64_t> &p2){
// 		return p1.second < p2.second || (p1.second == p2.second && p1.first < p2.first);
// 	}	
// }Cmptime;

struct 
{
	bool operator()(const SSTableCache* p1, const SSTableCache* p2){
		return p1->timeStamp > p2->timeStamp || (p1->timeStamp == p2->timeStamp && p1->minKey > p2->minKey);
	}
}TimeSort;

struct 
{
	bool operator()(const SSTableCache *p1, const SSTableCache *p2){
		return p1->maxKey <= p2->minKey;
	}
}ValueSort;

//load save后文件的读写？
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
	levelNum = 0;
	timeStamp = 0;
	int fileNum = 0;
	std::vector<std::string> allFile;
	if(!utils::dirExists(dir)){
		int ret = utils::_mkdir(dir.c_str());
	}
	else{
		levelNum = utils::scanDir(dir, allFile);
	}
	root = dir;
	// std::cout << levelNum << std::endl;  //这里确实是0
	if(levelNum != 0){
		for(int j = 0; j < levelNum; ++j){
			std::string dirName = "level-" + std::to_string(j);
			std::vector<SSTableCache*> nowlevel;
			allFile.clear();
			std::string pathName = dir + "/" + dirName;
			fileNum = utils::scanDir(pathName, allFile);
			sort(allFile.begin(), allFile.end(), Comp);
			// std::cout << "get here!" << std::endl;
			for(int i = 0; i < fileNum; ++i){
				//这里假设从磁盘中读入的文件名只是单纯的文件名，并不带相对路径
				std::string filePath = pathName + "/" + allFile[i];
				// std::cout << "fileName: " << filePath << std::endl;
				uint64_t tmpStamp = 0; 
				SSTableCache *cache = loadFile(filePath, tmpStamp, pathName);
				timeStamp = std::max(timeStamp, tmpStamp);
				nowlevel.emplace_back(cache);
			}
			Cache.emplace_back(nowlevel);
		}
	}
	MemTable = new SkipList();
	// std::cout << "init finished" << std::endl;
	//注意此时找到的是最后一个SSTable对应的时间戳
}

KVStore::~KVStore()
{
	int len = Cache.size();
	for(int i = 0; i < len; ++i){
		// std::cout << "get here !" << std::endl;
		if(Cache[i].empty()){
			Cache.pop_back();
			continue;
		}
		std::vector<SSTableCache*> delVec = Cache.back();
		Cache.pop_back();
		int num = delVec.size();
		for(int j = 0; j < num; ++j){
			SSTableCache *del = delVec.back();
			delVec.pop_back();
			delete del;
		}
	}
	
	//将Memtable现在有的内容写出来
	std::vector<std::pair<uint64_t, std::string>> li;
	uint64_t min = 0, max = 0;
	uint32_t size = 0;
	MemTable->WriteOut(min, max, size, li);
	if(!li.empty()){
		size = li.size();
		std::string dirPath = root + "/level-0";
		if(Cache.size() == 0 && levelNum == 0){
			utils::_mkdir(dirPath.c_str());
			Cache.emplace_back(std::vector<SSTableCache*>{});
			++levelNum;
		}
		++timeStamp;
		SSTable table;
		SSTableCache *newCache = table.SaveSSTable(dirPath, size, timeStamp, li, min, max);
		Cache[0].insert(Cache[0].begin(), newCache);
		compaction();
	}
	delete MemTable;
}

SSTableCache *KVStore::loadFile(const std::string &dir, uint64_t &timeStamp, const std::string &path){
	//注意这里获取的是当前最后一个时间戳
	SSTableCache *newCache = new SSTableCache(dir, timeStamp, path); 
	return newCache;
}
/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	// std::cout << "put val: " << key << std::endl;
	// if(key == 2029)
	// 	int k = 0;
	bool ret = MemTable->Insert(key, s);
	if(ret)
		return;
	std::vector<std::pair<uint64_t, std::string>> li;
	// if(s == "~DELETED~")
	std::cout << "save file key: " << key << std::endl;
	uint64_t min = 0, max = 0;
	uint32_t size = 0;
	try{
		MemTable->WriteOut(min, max, size, li);
	}
	catch(std::bad_alloc &ba){
			std::cout << "write out bug at 131" << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
	}
	// if(s.find('t') != std::string::npos)
	// 	std::cout << "t write get here! " << min << " " << max  << std::endl;
	// if(min == 0 && max == 9716)
	// 	std::cout << "t write get here! " << min << " " << max  << std::endl;
	size = li.size();
	delete MemTable;
	MemTable = new SkipList();
	std::string dirPath = root + "/level-0";
	if(Cache.size() == 0 && levelNum == 0){
		utils::_mkdir(dirPath.c_str());
		Cache.emplace_back(std::vector<SSTableCache*>{});
		++levelNum;
		std::cout << levelNum << std::endl;
	}
	++timeStamp;
	SSTable table;
	SSTableCache *newCache = table.SaveSSTable(dirPath, size, timeStamp, li, min, max);
	Cache[0].insert(Cache[0].begin(), newCache);
	// std::cout << "after insert!" << std::endl;
	//进行归并操作
	compaction();
	ret = MemTable->Insert(key, s);
	if(!ret)
		std::cout << "insert error!" << std::endl;
	// SSTableCache *newCache = new SSTableCache(dirPath, size, timeStamp, li, min, max);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string result;
	bool hasDel = false;
	if(key == 1310)
		int y = 1;
	bool ret = MemTable->Search(key, result, hasDel);
	// std::cout << key  << " " << ret << std::endl;
	if(ret || hasDel){
		// if(key < 500 && hasDel)
		// 	std::cout << "has delete: " << key << std::endl; 
		// if(key == 1)
		// 	std::cout << "check 1" << ret << " " << result << " " << hasDel << std::endl;
		return result;
	}
	// if(key == 1)
	// 	int l = 0;
	// SSTableCache *tofind;
	for(int i = 0; i < levelNum; ++i){
		std::vector<SSTableCache*> levelCache = Cache[i];
		int n = levelCache.size();
		uint32_t offset = 0, length = 0;
		bool isEnd = false;
		// std::list<SSTableCache*>::iterator it;
		// for(it=levelCache.begin();it!=levelCache.end();it++){
  		// 	SSTableCache* temp = *it;
		// 	if(temp->Search(key, offset, length, isEnd)){
				// std::cout << "is searching: " << j << std::endl; 
		// 		SSTable readTable;
		// 		std::string fileName = temp->toFileName();
		// 		result = readTable.ReadSSTable(offset, length, fileName, isEnd);
		// 		if(result != "~DELETED~")
		// 			return result;
		// 		else 
		// 			break;
		// 	}
 		// }
		for(int j = 0; j < n; ++j){
			// std::cout << "key: " << std::endl;
			if(levelCache[j]->Search(key, offset, length, isEnd)){
				// std::cout << "is searching: " << levelCache[j]->toFileName() << std::endl; 
				SSTable readTable(levelCache[j]);
				std::string fileName = levelCache[j]->toFileName();
				result = readTable.ReadSSTable(offset, length, fileName, isEnd);
				// if(key == 8104)
				// 	std::cout << "result: " << result << std::endl;
				if(result != "~DELETED~")
					return result;
				else if(result == "~DELETED~")
					return "";
			}
		}
	}
	result = "";
	return result;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	// if(key == 10000)
	// 	std::cout << "get line 209" << std::endl;
	std::string result = this->get(key);
	// std::cout << "result in delete: " << key << std::endl; 
	if(result == "")
		return false;
	this->put(key, "~DELETED~");
	// if(key < 100)
	// 	std::cout << "del key" << std::endl;
	return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	//先删除磁盘上的文件
	for(int i = 0; i < levelNum; ++i){
		std::string dirName = root + "/level-" + std::to_string(i);
		int n = Cache[i].size();
		//这里的allfile return的filename到底是不是带相对路径的？
		std::vector<std::string> allFile;
		int m = utils::scanDir(dirName, allFile);
		if(m != n){
			std::cout << "some SSTableCache don't have its file!" << std::endl;
			return;
		}
		for(int j = 0; j < m; ++j){
			std::string pathName = dirName + "/" + allFile[j];
			utils::rmfile(pathName.c_str());
		}
		utils::rmdir(dirName.c_str());
	}

	//然后把Memtable清除
	delete MemTable;
	MemTable = new SkipList();

	//然后清除缓存
	// std::list<SSTableCache*>::iterator it;
	for(int i = levelNum - 1; i >= 0; --i){
		int n = Cache[i].size();
		for(int j = n - 1; j >= 0; --j){
			delete Cache[i][j];
			Cache[i].pop_back();
		}
		// for(it=Cache[i].begin();it!=Cache[i].end();it++){
  		// 	delete *it;
 		// }
		Cache.pop_back();
	}	
	//这里对不对啊
	timeStamp = 0;
	levelNum = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
	list = MemTable->Scan(key1, key2);
	//还有SSTable中的内容，先不写了，用堆排序？
}

void KVStore::compaction()
{
	for(int i = 0; i < levelNum; ++i){
		int size = Cache[i].size();
		// std::cout << "compaction: " << i << " " << size << std::endl;
		// if(i == 1 && size == 7)
		// 	std::cout << "get here!" << std::endl; 
		if(size <= pow(2, i + 1))
			break;
		try{
			campactTwo(i);
		}
		catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug at 306" << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
		}
	}
}

bool isOverlap(std::pair<uint64_t, uint64_t> &interval, SSTableCache *s2)
{
	uint64_t Min = s2->minKey,  Max = s2->maxKey;
	//不重叠返回false,重叠返回true
	if(interval.second < Min || interval.first > Max)
		return false;
	else
		return true;
}

std::pair<uint64_t, uint64_t> computeLap(std::vector<SSTableCache*> &toDel)
{
	uint64_t Max = toDel[0]->maxKey, Min = toDel[0]->minKey;
	int len = toDel.size();
	for(int i = 1; i < len; ++i){
		Max = std::max(Max, toDel[i]->maxKey);
		Min = std::min(Min, toDel[i]->minKey);
	}
	return std::pair<uint64_t, uint64_t>{Min, Max};
}

void KVStore::campactTwo(int level)
{
	int p = 290;
	size_t exceedNum;
	std::vector<SSTableCache*> toDel(0);
	std::vector<SSTable*> Tlist(0); //顺便找到SSTable
	std::vector<std::pair<size_t, uint64_t>> timeArr(0);
	std::vector<std::string> toDelFileName(0);
	int hilevel = level + 1;
	std::string dirPath = root + "/level-" + std::to_string(level + 1);
	//找出level层需要合并下移的元素
	// std::cout << "get 345!" << std::endl;
	try{
		if(level == 0){
			exceedNum = Cache[0].size();
			toDel = Cache[0];
			Cache[0].clear();
		}
		else{
			p = 303;
			//按时间戳进行逆序排序
			sort(Cache[level].begin(), Cache[level].end(), TimeSort);
			size_t length = Cache[level].size();
			exceedNum = length - (int)pow(2, level + 1);
			p = 306;
			//加入SSTable和toDel
			for(size_t i = 1; i <= exceedNum; ++i){
				toDel.emplace_back(Cache[level][length - i]);
				// std::cout << Cache[level][length - i]->toFileName() << std::endl;
				// std::cout << "label: " << level << " " << length - i << std::endl; 
			}
			auto it = Cache[level].begin() + (length - exceedNum);
			//将这里多余的部分删除
			while(it != Cache[level].end()){
				std::cout << (*it)->toFileName() << std::endl; 
				it = Cache[level].erase(it);
			}
			p = 316;
			//按照键值进行排序
			sort(Cache[level].begin(), Cache[level].end(), ValueSort);
		}
	}
	catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug at 369" << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
	}
	//处理下一层的情况
	//如果不是最低层,寻找重叠的部分并将相应的cache删除
	try{
		if(level < levelNum - 1){
			std::cout << level << " " << levelNum << std::endl;
			std::cout << "size: " << Cache[hilevel].size() << std::endl; 
			std::vector<bool> visited(Cache[hilevel].size(), false);
			std::pair<uint64_t, uint64_t> interval = computeLap(toDel);
			size_t i = 0, n = Cache[hilevel].size();
			int hilevel = level + 1;
			while(i < n && !isOverlap(interval, Cache[hilevel][i])){
				++i;
			}
			while(i < n && isOverlap(interval, Cache[hilevel][i])){
				toDel.emplace_back(Cache[hilevel][i]);
				visited[i] = true;
				++i;
			}
			//寻找重叠的部分并将相应的cache删除
			i = 0;
			while(i < n){
				if(!visited[i]){
					++i;
					continue;
				}
				auto it = Cache[hilevel].begin() + i;
				while(it != Cache[hilevel].end() && i < n && visited[i]){
					it = Cache[hilevel].erase(it);
					++i;
				}	
			}
		}
		else{
			utils::_mkdir(dirPath.c_str());
			Cache.emplace_back(std::vector<SSTableCache*>{});
			++levelNum;
		}
	}
	catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug at 410" << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
	}
	//找到待删除的SSTable，并将原先的文件进行删除
	size_t size = toDel.size();
	for(size_t i = 0; i < size; ++i){
		std::string former = toDel[i]->toFileName();
		std::cout << "former name: " << former << std::endl;
		SSTable *newTable = nullptr;
		try{
			newTable = new SSTable(toDel[i]);
			size_t s = newTable->getEndlength();
			if(s == 0)
				std::cout << "kvstore.cc 366 length: " << s << std::endl;
		}
		catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug " << "at" << p << std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
		}
		Tlist.emplace_back(newTable);
		toDel[i]->addr = dirPath;
		utils::rmfile(former.c_str());
	}
	//找到相应的SSTable并进行归并
	try{
		SSTable tempTable;
		p = 376;
		tempTable.mergeSort(Tlist);
		//将相应的cache进行存储
		Tlist[0]->setAddr(dirPath);
		p = 381;
		std::vector<SSTableCache*> caches = tempTable.division(Tlist[0]);	
		p = 383;
		size = Cache[level + 1].size();
		for(auto it: caches){
			std::cout << "after merge the file: " << it->toFileName() << std::endl;  
		}
		Cache[hilevel].insert(Cache[hilevel].end(), caches.begin(), caches.end());
		sort(Cache[hilevel].begin(), Cache[hilevel].end(), ValueSort);
		std::vector<SSTableCache*>().swap(toDel);
		size_t lens = Tlist.size();
		for(size_t k = 0; k < lens; ++k){
			SSTable *tmp = Tlist.back();
			Tlist.pop_back();
			delete tmp;
		}
	}
	catch(std::bad_alloc &ba){
			std::cout << "compaction two level bug at 457"<< std::endl;
			std::cout << ba.what() << std::endl;
			exit(0);
	}
	std::vector<SSTable*>().swap(Tlist);
	std::vector<std::pair<size_t, uint64_t>>().swap(timeArr);
	std::vector<std::string>().swap(toDelFileName);	
}