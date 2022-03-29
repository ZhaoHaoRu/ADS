#pragma once

#include <vector>
#include <utility>
#include <cmath>
#include <string>
#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"

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
};
