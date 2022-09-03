#pragma once

#include <vector>
#include <utility>
#include <cmath>
#include <string>
#include <memory>
#include <fstream>
#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"

struct Pointer{
	SKNode *node;
	uint64_t key;
	uint64_t pos;
	uint32_t length;
	int nowLevel;
	bool isEnd;	//判断是否是当前的文件的结尾
	int ind;	//ind指示的是pointer指向的文件在当前层的id
	uint64_t timeStamp; //timeStamp因为会出现重复的键值对，选取时间戳大的那一个
	Pointer(SKNode *n = nullptr, uint64_t k = 0, uint64_t p = 0, uint32_t len = 0, bool end = false, int Level = -1, int index = -1, uint64_t tst = -1)
	{
		pos = p;
		key = k;
		node = n;
		length = len;
		isEnd = end;
		nowLevel = Level;
		ind = index;
		timeStamp = tst;
	}
};

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	SkipList* MemTable;
	//levelNum是从1开始计数的
	int levelNum;
	std::string root;
	uint64_t timeStamp;
	std::vector<std::vector<SSTableCache*>> Cache;

public:
	KVStore(const std::string &dir);

	~KVStore();

	//读取现有的SSTable,将数据区之外的其他部分载入内存，找到最后一个SSTable对应的时间戳
	SSTableCache *loadFile(const std::string &dir, uint64_t &timeStamp, const std::string &path);

	// 合并操作
	void compaction();

	// 合并两层
	void campactTwo(int level);

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

	bool findHead(uint64_t key, uint32_t offset,int level, uint64_t key1, uint64_t key2, int &ind, bool &isEnd, uint32_t &length);
};
