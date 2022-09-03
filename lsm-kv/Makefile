
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -g

all: correctness persistence regularTest

correctness: kvstore.o correctness.o BloomFilter.o SSTable.o SkipList.o

persistence: kvstore.o persistence.o BloomFilter.o SSTable.o SkipList.o

regularTest: kvstore.o regularTest.o BloomFilter.o SSTable.o SkipList.o

clean:
	-rm -f correctness persistence *.o
