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

#include "mapwriter.h"

#ifdef USE_ONE_PHASE_IO
#include "infinimem/onePhaseFileIO.hpp"
#else
#include "infinimem/twoPhaseFileIO.hpp"
#endif

#include <vector>
#include <ctype.h> 
#include <algorithm> 
#include <stack> 
#include <fstream>  
#include<iostream> 
#include<iterator> 
#include <utility>
#include <unistd.h>

  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::initBuf(unsigned nMappers, unsigned nReducers, unsigned bSize, unsigned kItems) {
  nRows = nMappers;
  nCols = nReducers;
  writtenToDisk = false;
  batchSize = bSize;
  kBItems = kItems;
  totalKeysInFile = new IdType[nCols];
  nItems = new IdType[nRows * nCols];

  outBufMap = new InMemoryContainer<KeyType, ValueType>[nRows * nCols];
  readBufMap = new InMemoryContainer<KeyType, ValueType>[nCols];
  lookUpTable = new LookUpTable<KeyType>[nCols];
  fetchBatchIds = new std::set<unsigned>[nCols];
  readNextInBatch = new std::vector<unsigned long long>[nCols];
  batchesCompleted = new std::vector<bool>[nCols];
  keysPerBatch = new std::vector<unsigned>[nCols];

  for (unsigned i=0; i<nRows * nCols; ++i) 
    nItems[i] = 0;

  for(unsigned i=0; i<nCols; ++i) {  
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    locks.push_back(mutex);

    totalKeysInFile[i] = 0;
  }

  char cwd[1024];
  if (getcwd(cwd, sizeof(cwd)) == NULL) {
    perror("getcwd() error");
    exit(-1);
  }

  sprintf(cwd + strlen(cwd), "/tmp/");

#ifdef USE_ONE_PHASE_IO
  io = new OnePhaseFileIO<RecordType>(cwd, nCols);
#else
  io = new TwoPhaseFileIO<RecordType>(cwd, nCols);
#endif
}

  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::releaseMapStructures() {
  for (unsigned i = 0; i < nCols; i++)
    pthread_mutex_destroy(&locks[i]);

  for (unsigned i = 0; i < nRows * nCols; i++)
    outBufMap[i].clear();

  delete[] nItems;
  delete[] outBufMap;
}

  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::shutdown() {
  delete io;
  readBufMap->clear();

  delete[] totalKeysInFile;
  delete[] readBufMap;
  delete[] lookUpTable;
  delete[] fetchBatchIds;
  delete[] readNextInBatch;
  delete[] batchesCompleted;
  delete[] keysPerBatch;
}

template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::writeBuf(const unsigned tid, const KeyType& key, const ValueType& value) {
  unsigned bufferId = hashKey(key) % nCols; 
  unsigned buffer = tid * nCols + bufferId;  

  if (outBufMap[buffer].size() >= batchSize) {
    pthread_mutex_lock(&locks[bufferId]);
    writeToInfinimem(bufferId, totalKeysInFile[bufferId], outBufMap[buffer].size(), outBufMap[buffer]);
    totalKeysInFile[bufferId] += nItems[buffer];
    pthread_mutex_unlock(&locks[bufferId]);

    outBufMap[buffer].clear();
    nItems[buffer] = 0;
    writtenToDisk = true;
  }

  performWrite(tid, buffer, key, value);
}

template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::flushMapResidues(const unsigned tid) {
  if(tid >= nCols) 
    return;

  if(nRows == 1) {
    writeToInfinimem(tid, totalKeysInFile[tid], static_cast<unsigned>(outBufMap[tid].size()), outBufMap[tid]);
    outBufMap[tid].clear();
    totalKeysInFile[tid] += nItems[tid];
    nItems[tid] = 0;
  }
  else {
    unsigned b1 = 0, b2 = 0;
    InMemoryContainerIterator<KeyType, ValueType> b2Iter, b2End;
    unsigned long long b2Merged = 0;
    bool findB1 = true, findB2 = true;
    unsigned i = 0;
    while(i < nRows - 1) {
      if(findB1) {
        b1 = (i * nCols) + tid;
        findB1 = false;
        ++i;
      }
      if(findB2) {
        b2 = (i * nCols) + tid;
        b2Iter = outBufMap[b2].begin();
        b2End = outBufMap[b2].end();
        b2Merged = 0;
        findB2 = false;
        ++i;
      }

      b2Merged += merge(outBufMap[b1], b1, tid, b2Iter, b2End);

      if(outBufMap[b1].size() == 0) {
        findB1 = true;
      }
      if(b2Iter == b2End) {
        findB2 = true;
      }
    }

    if(i == nRows - 1) {
      if(findB1 && findB2) {
        writeToInfinimem(tid, totalKeysInFile[tid], outBufMap[i].size(), outBufMap[i]);
        outBufMap[i].clear();
        totalKeysInFile[tid] += nItems[i];
        nItems[i] = 0;
      }
      else if(findB1 || findB2) {
        if(findB1) {
          b1 = (i * nCols) + tid;
          findB1 = false;
          ++i;
        } else {
          b2 = (i * nCols) + tid;
          b2Iter = outBufMap[b2].begin();
          b2End = outBufMap[b2].end();
          b2Merged = 0;
          findB2 = false;
          ++i;
        }  

        b2Merged += merge(outBufMap[b1], b1, tid, b2Iter, b2End);
        if(outBufMap[b1].size() == 0) {
          if(b2Iter != b2End) {
            bWriteToInfinimem(tid, totalKeysInFile[tid], outBufMap[b2].size() - b2Merged, b2Iter, b2End);
            outBufMap[b2].clear();
            totalKeysInFile[tid] += (nItems[b2] - b2Merged);
            nItems[b2] = 0;
          }
        } 
        if(b2Iter == b2End) {
          if(outBufMap[b1].size() != 0) {
            writeToInfinimem(tid, totalKeysInFile[tid], outBufMap[b1].size(), outBufMap[b1]);
            outBufMap[b1].clear();
            totalKeysInFile[tid] += nItems[b1];
            nItems[b1] = 0;
          }
        }
      } else
          assert(false);
    } else if(i == nRows) {
      if(outBufMap[b1].size() > 0) {
        writeToInfinimem(tid, totalKeysInFile[tid], outBufMap[b1].size(), outBufMap[b1]);
        outBufMap[b1].clear();
        totalKeysInFile[tid] += nItems[b1];
        nItems[b1] = 0;
      } else {  
        bWriteToInfinimem(tid, totalKeysInFile[tid], outBufMap[b2].size() - b2Merged, b2Iter, b2End);
        outBufMap[b2].clear();
        totalKeysInFile[tid] += (nItems[b2] - b2Merged);
        nItems[b2] = 0;
      }
    } else
      assert(false);
  }
}

template <typename KeyType, typename ValueType>
unsigned long long MapWriter<KeyType, ValueType>::merge(InMemoryContainer<KeyType, ValueType>& toMap, unsigned whichMap, unsigned tid, InMemoryContainerIterator<KeyType, ValueType>& begin, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  unsigned long long ct = 0;
  while(begin != end) {
    if(toMap.size() >= batchSize) {
      writeToInfinimem(tid, totalKeysInFile[tid], toMap.size(), toMap);
      toMap.clear();
      totalKeysInFile[tid] += nItems[whichMap];
      nItems[whichMap] = 0; 
      return ct;
    }
    InMemoryContainerIterator<KeyType, ValueType> it = toMap.find(begin->first);
    if(it != toMap.end())
      combine(it->first, it->second, begin->second);
    else {
      toMap.emplace(begin->first, begin->second);
      ++nItems[whichMap];
    }
    ++ct;
    ++begin;
  }
  return ct;
}

  template <typename KeyType, typename ValueType> 
void MapWriter<KeyType, ValueType>::performWrite(const unsigned tid, const unsigned buffer, const KeyType& key, const ValueType& value) {
  std::vector<ValueType> vals(1, value);
  InMemoryContainerIterator<KeyType, ValueType> it = outBufMap[buffer].find(key); 

  if(it != outBufMap[buffer].end())
    combine(key, it->second, vals);
  else {
    outBufMap[buffer].emplace(key, vals); 
    nItems[buffer]++;
  }
}

template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::writeToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, const InMemoryContainer<KeyType, ValueType>& inMemMap) {
  RecordType* records = new RecordType[noItems]; 
  unsigned ct = 0;

  for (InMemoryContainerConstIterator<KeyType, ValueType> it = inMemMap.begin(); it != inMemMap.end(); ++it) {
    records[ct].set_key(it->first);
#ifdef USE_ONE_PHASE_IO
    assert(it->second.size() == 1);
    records[ct].set_value(it->second[0]);
#else
    for (typename std::vector<ValueType>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit)
      records[ct].add_values(*vit);
#endif
    ++ct;
  }
  assert(ct == noItems);
  io->file_set_batch(buffer, startKey, noItems, records);

  delete[] records;
}

template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::bWriteToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> begin, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  RecordType* records = new RecordType[noItems]; 
  unsigned ct = 0;

  for (InMemoryContainerConstIterator<KeyType, ValueType> it = begin; it != end; ++it) {
    records[ct].set_key(it->first);
#ifdef USE_ONE_PHASE_IO
    assert(it->second.size() == 1);
    records[ct].set_value(it->second[0]);
#else
    for (typename std::vector<ValueType>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit)
      records[ct].add_values(*vit);
#endif
    ++ct;
  }

  assert(ct == noItems);
  io->file_set_batch(buffer, startKey, noItems, records);

  delete[] records;
}


  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::readInit(const unsigned tid) {
  unsigned j=0;
  for (unsigned long long i = 0; i <= totalKeysInFile[tid]; i+= batchSize) {
    readNextInBatch[tid].push_back(i); 
    fetchBatchIds[tid].insert(j++); 
    batchesCompleted[tid].push_back(false); 
    keysPerBatch[tid].push_back(kBItems); 
  }
}


template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::read(const unsigned tid, InMemoryContainer<KeyType, ValueType>& readBufMap, std::vector<unsigned>& keysPerBatch, LookUpTable<KeyType>& lookUpTable, std::set<unsigned>& fetchBatchIds, std::vector<unsigned long long>& readNextInBatch, std::vector<bool>& batchesCompleted) {
  RecordType* records = new RecordType[kBItems];
  unsigned batch = 0;
  for(auto it = fetchBatchIds.begin(); it != fetchBatchIds.end(); ++it) {
    batch = *it ;
    unsigned long long batchBoundary = std::min(static_cast<unsigned long long>((batch + 1) * batchSize), static_cast<unsigned long long>(totalKeysInFile[tid]));

    if (readNextInBatch[batch] >= batchBoundary) {
      batchesCompleted[batch] = true;
      continue;
    }

    keysPerBatch[batch] = std::min(keysPerBatch[batch], static_cast<unsigned>(batchBoundary - readNextInBatch[batch]));

    if (keysPerBatch[batch] > 0 && readNextInBatch[batch] < batchBoundary)
      io->file_get_batch(tid, readNextInBatch[batch], keysPerBatch[batch], records); 

    for (unsigned i = 0; i < keysPerBatch[batch]; i++) {
      lookUpTable[records[i].key()].push_back(batch);
      readBufMap[records[i].key()];
#ifdef USE_ONE_PHASE_IO
      readBufMap[records[i].key()].push_back(records[i].value());
#else
      for (unsigned k = 0; k < records[i].values_size(); k++)
        readBufMap[records[i].key()].push_back(records[i].values(k));
#endif
    }

    readNextInBatch[batch] += keysPerBatch[batch];
    keysPerBatch[batch] = 0;

    if (readNextInBatch[batch] >= batchBoundary)
      batchesCompleted[batch] = true;
  }

  fetchBatchIds.clear();

  bool ret = false;
  for (unsigned readBatch = 0; readBatch < batchesCompleted.size(); readBatch++)
    if (batchesCompleted[readBatch] == false) {
      ret = true;
      break;
    }

  delete[] records;
  return ret;
}

template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::read(const unsigned tid) {
  return read(tid, readBufMap[tid], keysPerBatch[tid], lookUpTable[tid], fetchBatchIds[tid], readNextInBatch[tid], batchesCompleted[tid]);
}

template <typename KeyType, typename ValueType>
InMemoryReductionState<KeyType, ValueType> MapWriter<KeyType, ValueType>::initiateInMemoryReduce(unsigned tid) {
  InMemoryReductionState<KeyType, ValueType> state(nRows); 
  for(unsigned i=0; i<nRows; ++i) {
    state.begins[i] = outBufMap[tid + nCols * i].begin();
    state.ends[i] = outBufMap[tid + nCols * i].end();
  }

  return state;
}

template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::getNextMinKey(InMemoryReductionState<KeyType, ValueType>* state, InMemoryContainer<KeyType, ValueType>* record) {
  std::vector<unsigned> minIds;
  KeyType minKey;
  bool found = false;

  for(unsigned i=0; i<nRows; ++i) {
    if(state->begins[i] == state->ends[i])
      continue;

    if(!found) {
      minKey = state->begins[i]->first;
      minIds.push_back(i);
      found = true;
    } else {
      if(state->begins[i]->first < minKey) {
        minKey = state->begins[i]->first;
        minIds.clear();
        minIds.push_back(i);
      } else if(state->begins[i]->first == minKey) {
        minIds.push_back(i);
      }
    }
  }

  if(!found)
    return false;

  std::vector<ValueType>& values = (*record)[minKey];
  for(typename std::vector<unsigned>::iterator it = minIds.begin(); it != minIds.end(); ++it) {
    for(typename std::vector<ValueType>::const_iterator vit = state->begins[*it]->second.begin(); vit != state->begins[*it]->second.end(); ++vit) 
      values.push_back(*vit);

    ++state->begins[*it];
  }

  return true;
}
