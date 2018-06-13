/*
MIT License

Copyright (c) 2018 Keval Vora <keval@cs.sfu.ca>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef __MAPWRITER_H__
#define __MAPWRITER_H__
#include "infinimem/fileIO.h"
#include <utility> 
#include <map>
#include <set>
#include <vector>
#include <stack>
#include <queue>

#ifdef USE_STRING_HASH
#define hashKey(str) stringHash(str)
#endif

#ifdef USE_NUMERICAL_HASH
#define hashKey(number) (number)
#endif

template <typename KeyType, typename ValueType>
using InMemoryContainer = std::map<KeyType, std::vector<ValueType> >;

template <typename KeyType, typename ValueType>
using InMemoryContainerIterator = typename InMemoryContainer<KeyType, ValueType>::iterator; 

template <typename KeyType, typename ValueType>
using InMemoryContainerConstIterator = typename InMemoryContainer<KeyType, ValueType>::const_iterator;

template <typename KeyType>
using LookUpTable = std::map<KeyType, std::vector<unsigned> >;

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from);

template <typename KeyType, typename ValueType>
class InMemoryReductionState {
  public:
  std::vector<InMemoryContainerConstIterator<KeyType, ValueType> > begins;
  std::vector<InMemoryContainerConstIterator<KeyType, ValueType> > ends;

  InMemoryReductionState(unsigned size) : begins(size), ends(size) { }
};

template <typename KeyType, typename ValueType>
class MapWriter {
  public:
    void initBuf(unsigned nMappers, unsigned nReducers, unsigned bSize, unsigned kItems);
    void writeBuf(const unsigned tid, const KeyType& key, const ValueType& value); 
    void flushMapResidues(const unsigned tid);

    unsigned long long merge(InMemoryContainer<KeyType, ValueType>& toMap, unsigned whichMap, unsigned tid, InMemoryContainerIterator<KeyType, ValueType>& begin, InMemoryContainerConstIterator<KeyType, ValueType> end);
    void bWriteToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> begin, InMemoryContainerConstIterator<KeyType, ValueType> end);

    void performWrite(const unsigned tid, const unsigned buffer, const KeyType& key, const ValueType& value); 
    void writeToInfinimem(const unsigned buffer, const IdType startKey, unsigned nItems, const InMemoryContainer<KeyType, ValueType>& inMemMap); 
    bool read(const unsigned tid);
    void readInit(const unsigned tid);
    void releaseMapStructures();
    void shutdown();

    bool getWrittenToDisk() { return writtenToDisk; }

    InMemoryReductionState<KeyType, ValueType> initiateInMemoryReduce(unsigned tid);
    bool getNextMinKey(InMemoryReductionState<KeyType, ValueType>* state, InMemoryContainer<KeyType, ValueType>* record);

    InMemoryContainer<KeyType, ValueType>* readBufMap;
    LookUpTable<KeyType>* lookUpTable;
    std::set<unsigned>* fetchBatchIds;

    std::vector<unsigned long long>* readNextInBatch;
    std::vector<bool>* batchesCompleted;
    std::vector<unsigned>* keysPerBatch;

  private:
    bool read(const unsigned tid, InMemoryContainer<KeyType, ValueType>& readBufMap, std::vector<unsigned>& keysPerBatch, LookUpTable<KeyType>& lookUpTable, std::set<unsigned>& fetchBatchIds, std::vector<unsigned long long>& readNextInBatch, std::vector<bool>& batchesCompleted);
    
    unsigned nRows;
    unsigned nCols;
    unsigned batchSize;  
    unsigned kBItems;  
    bool firstInit;
    IdType* nItems; 
    InMemoryContainer<KeyType, ValueType>* outBufMap;  

    IdType* totalKeysInFile;
    std::vector<pthread_mutex_t> locks;
    FileIO<RecordType> *io;  
    bool writtenToDisk;
};

#endif // __MAPWRITER_H__

