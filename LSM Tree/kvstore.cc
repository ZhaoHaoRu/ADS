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
		std::cout << num1 << " " << num2 << std::endl;
		return num1 > num2;
	}
}Comp;

struct 
{
	bool operator()(const std::pair<size_t, uint64_t> &p1, const std::pair<size_t, uint64_t> &p2){
		return p1.second < p2.second || (p1.second == p2.second && p1.first < p2.first);
	}	
}Cmptime;

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
				std::cout << "fileName: " << filePath << std::endl; 
				SSTableCache *cache = loadFile(filePath, timeStamp, pathName);
				nowlevel.push_back(cache);
			}
			Cache.push_back(nowlevel);
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
		std::vector<SSTableCache*> delVec = Cache.back();
		Cache.pop_back();
		int num = delVec.size();
		for(int j = 0; j < num; ++j){
			// SSTableCache *del = delVec.back();
			Cache.pop_back();
			// delete del;
		}
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
	// std::cout << "put key: " << key << std::endl;
	if(key == 3525)
		int k = 0;
	bool ret = MemTable->Insert(key, s);
	if(ret)
		return;
	std::vector<std::pair<uint64_t, std::string>> li;
	// if(s.find('t') != std::string::npos)
	// 	std::cout << "t write get here!" << std::endl;
	uint64_t min = 0, max = 0;
	uint32_t size = 0;
	MemTable->WriteOut(min, max, size, li);
	size = li.size();
	delete MemTable;
	MemTable = new SkipList();
	std::string dirPath = root + "/level-0";
	if(Cache.size() == 0){
		utils::_mkdir(dirPath.c_str());
		Cache.push_back(std::vector<SSTableCache*>{});
		++levelNum;
	}
	++timeStamp;
	SSTable table;
	SSTableCache *newCache = table.SaveSSTable(dirPath, size, timeStamp, li, min, max);
	Cache[0].insert(Cache[0].begin(), newCache);
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
	if(key == 5922)
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
		// 		// std::cout << "is searching: " << j << std::endl; 
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
				// std::cout << "is searching: " << j << std::endl; 
				SSTable readTable(levelCache[j]);
				std::string fileName = levelCache[j]->toFileName();
				result = readTable.ReadSSTable(offset, length, fileName, isEnd);
				if(result != "~DELETED~")
					return result;
				else
					break;
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
	std::list<SSTableCache*>::iterator it;
	for(int i = 0; i < levelNum; ++i){
		int n = Cache[i].size();
		for(int j = 0; j < n; ++j){
			delete Cache[i][j];
		}
		// for(it=Cache[i].begin();it!=Cache[i].end();it++){
  		// 	delete *it;
 		// }
	}
		
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
		std::cout << "compaction: " << i << " " << size << std::endl;
		if(size <= pow(2, i + 1))
			break;
		campactTwo(i);
	}
}

bool isOverlap(std::pair<uint64_t, uint64_t> &interval, SSTableCache *s2)
{
	uint64_t Min = s2->minKey,  Max = s2->maxKey;
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
	int exceedNum;
	std::vector<SSTableCache*> toDel;
	std::vector<SSTable> Tlist; //顺便找到SSTable
	std::vector<std::pair<size_t, uint64_t>> timeArr;
	std::vector<std::string> toDelFileName;
	if(level == 0){
		exceedNum = Cache[0].size();
		toDel = Cache[0];
		Cache[0].clear();
	}
	else{
		size_t length = Cache[level].size();
		exceedNum = length - (int)pow(2, level + 1);
		for(size_t i = 0; i < length; ++i)
			timeArr.emplace_back(std::pair<size_t, uint64_t>(i, Cache[level][i]->timeStamp));
		sort(timeArr.begin(), timeArr.end(), Cmptime);
		for(int i = 0; i < exceedNum; ++i){
			toDel.push_back(Cache[level][timeArr[i].first]);
			//感觉这里erase应该有问题，但是还是有点晕啊
			Cache[level].erase(Cache[level].begin() + timeArr[i].first);
		}
	}
	if(level == levelNum - 1){
		std::string dirPath = root + "/level-" + std::to_string(levelNum);
		utils::_mkdir(dirPath.c_str());
		Cache.push_back(std::vector<SSTableCache*>{});
		++levelNum;
		size_t length = toDel.size();
		//写成SSTable并保存,删除原有的SSTable,并将SSTableCache进行保存
		for(size_t i = 0; i < length; ++i){
			std::string former = toDel[i]->toFileName();
			SSTable newTable(toDel[i]);
			toDel[i]->addr = dirPath;
			newTable.SSTableToFile(toDel[i]);
			utils::rmfile(former.c_str());
			Cache[levelNum - 1].emplace_back(toDel[i]);
		}
		return;
	}
	//找到待删除的SSTable，并将原先的文件进行删除
	size_t size = toDel.size();
	for(size_t i = 0; i < size; ++i){
		std::string former = toDel[i]->toFileName();
		Tlist.emplace_back(SSTable(toDel[i]));
		toDel[i]->addr = root + "/level-" + std::to_string(level + 1);
		utils::rmfile(former.c_str());
	}
	//将重叠的部分加入待删除部分
	std::pair<uint64_t, uint64_t> interval = computeLap(toDel);
	// for(int i = 0; i < Cache[1].size(); ++i){
	// 	if(Cache[1][i]->minKey > interval.second || Cache[1][i]->maxKey < interval.first)
	// 		continue;
	// 	toDel.push_back(Cache[1][i]);
	// }
	size_t i = 0;
	int hilevel = level + 1;
	bool hasCompat = false;
	while(i < Cache[hilevel].size()){
		if(!isOverlap(interval, Cache[hilevel][i])){
			++i;
			hasCompat = true;
			continue;
		}
		std::string former = Cache[hilevel][i]->toFileName();
		toDel.push_back(Cache[hilevel][i]);
		Tlist.emplace_back(SSTable(toDel.back()));
		Cache[hilevel].erase(Cache[hilevel].begin() + i);
		utils::rmfile(former.c_str());
	}
	//如果没有重叠的话，可以直接放进去
	// if(!hasCompat){

	// }
	//找到相应的SSTable并进行归并
	SSTable tempTable;
	tempTable.mergeSort(Tlist);
	//将相应的cache进行存储
	std::string tmp = root + "/level-" + std::to_string(level + 1);
	Tlist[0].setAddr(tmp);
	std::vector<SSTableCache*> caches = tempTable.division(Tlist[0]);
	size = Cache[hilevel].size();
	uint64_t Minkey = caches[0]->minKey;
	size_t j = 0;
	while(j < size && Minkey > Cache[hilevel][j]->maxKey){
		++j;
	}
	if(j == size)
		Cache[hilevel].insert(Cache[hilevel].end(), caches.begin(), caches.end());
	else
		Cache[hilevel].insert(Cache[hilevel].begin() + j, caches.begin(), caches.end());
}