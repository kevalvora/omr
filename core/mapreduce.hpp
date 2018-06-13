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

#include "mapreduce.h"
#include "mapwriter.hpp"
#include <utility> 
#include <fstream>
#include <iostream> 
#include <pthread.h>
#include <atomic>
#include <vector>
#include <string>
#include <ctime>

void parallelExecute(void *(*func)(void *), void* arg, unsigned threads) {
  pthread_t thread_handles[threads];
  std::pair<unsigned, void*> args[threads];

  for (unsigned i = 0; i < threads; i++) {
    args[i] = std::make_pair(i, arg);
    pthread_create(&thread_handles[i], NULL, func, &args[i]);
  }

  for (unsigned i = 0; i < threads; i++)
    pthread_join(thread_handles[i], NULL);
}

  template <typename KeyType, typename ValueType>
void* doMap(void* arg) {
  double time_map = 0.0;
  unsigned tid = static_cast<unsigned>(static_cast<std::pair<unsigned, void*>*>(arg)->first);
  MapReduce<KeyType, ValueType> *mr = static_cast<MapReduce<KeyType, ValueType> *>(static_cast<std::pair<unsigned, void*>*>(arg)->second);
  MapWriter<KeyType, ValueType>& writer = mr->writer;

  mr->beforeMap(tid);

  static std::atomic<unsigned> nextFileId(0);
  unsigned fileId = 0;
  while((fileId = nextFileId++) < mr->fileList.size()) {
    std::string fname = mr->inputFolder + "/" + mr->fileList.at(fileId);
    //fprintf(stderr, "thread %u working on file %d which is %s\n", tid, fileId, fname.c_str());
    std::ifstream infile(fname.c_str());
    assert(infile.is_open());
    std::string line;
    while(std::getline(infile, line)) {
      time_map -= getTimer();
      mr->map(tid, fileId, line);
      time_map += getTimer();
    }
  }

  time_map -= getTimer();
  pthread_barrier_wait(&(mr->barMap));
  if(writer.getWrittenToDisk())
    mr->writer.flushMapResidues(tid);
  mr->afterMap(tid);
  time_map += getTimer();
  mr->map_times[tid] = time_map;

  return NULL;
}

  template <typename KeyType, typename ValueType>
void* doReduce(void* arg) {
  double time_reduce = -getTimer();

  unsigned tid = static_cast<unsigned>(static_cast<std::pair<unsigned, void*>*>(arg)->first);
  MapReduce<KeyType, ValueType> *mr = static_cast<MapReduce<KeyType, ValueType> *>(static_cast<std::pair<unsigned, void*>*>(arg)->second);
  MapWriter<KeyType, ValueType>& writer = mr->writer; 

  mr->beforeReduce(tid);

  mr->readInit(tid);

  while(true) {
    bool execLoop = mr->read(tid);
    if(execLoop == false) {
      for(InMemoryContainerConstIterator<KeyType, ValueType> it = writer.readBufMap[tid].begin(); it != writer.readBufMap[tid].end(); ++it)
        mr->reduce(tid, it->first, it->second);
      break;
    }

    unsigned counter = 0; 
    InMemoryContainerIterator<KeyType, ValueType> it;
    for (it = writer.readBufMap[tid].begin(); it != writer.readBufMap[tid].end(); ++it) {
      if (counter >= mr->kBItems)
        break;

      mr->reduce(tid, it->first, it->second);

      const KeyType& key = it->first;
      auto pos = writer.lookUpTable[tid].find(key);
      assert(pos != writer.lookUpTable[tid].end());
      const std::vector<unsigned>& lookVal = pos->second;
      for(unsigned val=0; val<lookVal.size(); val++) {
        writer.fetchBatchIds[tid].insert(lookVal[val]);
        writer.keysPerBatch[tid][lookVal[val]] += 1; 
      }
      writer.lookUpTable[tid].erase(key);
      counter++;
    }

    writer.readBufMap[tid].erase(writer.readBufMap[tid].begin(), it);
  }

  mr->afterReduce(tid);

  time_reduce += getTimer();
  mr->reduce_times[tid] += time_reduce;

  return NULL;
}

template <typename KeyType, typename ValueType>
void* doInMemoryReduce(void* arg) {
  double time_reduce = -getTimer();

  unsigned tid = static_cast<unsigned>(static_cast<std::pair<unsigned, void*>*>(arg)->first);
  MapReduce<KeyType, ValueType> *mr = static_cast<MapReduce<KeyType, ValueType> *>(static_cast<std::pair<unsigned, void*>*>(arg)->second);
  MapWriter<KeyType, ValueType>& writer = mr->writer;

  mr->beforeReduce(tid);
  InMemoryReductionState<KeyType, ValueType> state = writer.initiateInMemoryReduce(tid); 
  InMemoryContainer<KeyType, ValueType> record;
  while(writer.getNextMinKey(&state, &record)) {
    mr->reduce(tid, record.begin()->first, record.begin()->second);
    record.clear();
  }
  mr->afterReduce(tid);
  time_reduce += getTimer();
  mr->reduce_times[tid] += time_reduce;

  return NULL;
}

  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::run() {
  double run_time = -getTimer();
  writer.initBuf(nMappers, nReducers, batchSize, kBItems);

  map_times.resize(nMappers, 0.0);
  reduce_times.resize(nReducers, 0.0);

  fprintf(stderr, "Running Mappers\n");
  parallelExecute(doMap<KeyType, ValueType>, this, nMappers);

  if(!writer.getWrittenToDisk()) {
    fprintf(stderr, "Running InMemoryReducers\n");
    parallelExecute(doInMemoryReduce<KeyType, ValueType>, this, nReducers);
    writer.releaseMapStructures();
  } else {
    writer.releaseMapStructures();
    fprintf(stderr, "Running Reducers\n");
    parallelExecute(doReduce<KeyType, ValueType>, this, nReducers);
  }

  run_time += getTimer();

  fprintf(stderr, "--------------------------------------\n");
  auto map_time = max_element(std::begin(map_times), std::end(map_times)); 
  std::cout << " Map time : " << *map_time << " msec" << std::endl;

  auto reduce_time = max_element(std::begin(reduce_times), std::end(reduce_times));
  std::cout << " Reduce time : " << *reduce_time << " msec" << std::endl;

  std::cout << " Map+Reduce time : " << ((*map_time) + (*reduce_time)) << " msec" << std::endl;
  std::cout << " Total time (including ingress) : " << run_time << " msec" << std::endl;
  fprintf(stderr, "--------------------------------------\n");
}

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::init(const std::string input, const unsigned mappers, const unsigned reducers, const unsigned bSize, const unsigned kItems) {
  inputFolder = input;
  nMappers = mappers;
  nReducers = reducers;
  batchSize = bSize;
  kBItems = kItems;

  getListOfFiles(inputFolder, &fileList);
  std::cout << "Number of files: " << fileList.size() << std::endl;

  if(fileList.size() == 0) {
    std::cout << "No work to be done" << std::endl;
    exit(0);
  }

  nMappers = std::min(static_cast<unsigned>(fileList.size()), nMappers);
  nReducers = std::min(nMappers, nReducers);

  std::cout << "nMappers: " << nMappers << std::endl;
  std::cout << "nReducers: " << nReducers << std::endl;
  std::cout << "batchSize: " << batchSize << std::endl;
  std::cout << "topk: " << kBItems << std::endl;

  pthread_barrier_init(&barMap, NULL, nMappers);
  pthread_barrier_init(&barReduce, NULL, nReducers);
}

  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::writeBuf(const unsigned tid, const KeyType& key, const ValueType& value) {
  writer.writeBuf(tid, key, value);
}

template <typename KeyType, typename ValueType>
bool MapReduce<KeyType, ValueType>::read(const unsigned tid) {
  return writer.read(tid);
}

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::readInit(const unsigned tid) {
  return writer.readInit(tid);
}
