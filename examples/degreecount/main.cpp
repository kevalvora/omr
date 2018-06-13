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

#ifdef USE_ONE_PHASE_IO
#include "data-one.pb.h"
#else
#include "data.pb.h"
#endif

#define USE_NUMERICAL_HASH

#include "../../core/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include <limits>

static std::string outputPrefix = "";
const uint64_t singleoutdegree = 1;
const uint64_t singleindegree = static_cast<uint64_t>(1) << 32;

// Two uint32_t packed in one uint64_t such that indegree is on MSBs and outdegree is on LSBs
template <typename KeyType, typename ValueType>
class DegreeCount : public MapReduce<KeyType, ValueType>
{
  public:
  void* map(const unsigned tid, const unsigned fileId, const std::string& input) {
    if(input[0] == '%' || input[0] == '#')
      return NULL;

    std::stringstream inputStream(input);
    uint32_t src, dst;
    inputStream >> src >> dst;

    this->writeBuf(tid, src, singleoutdegree);
    this->writeBuf(tid, dst, singleindegree);
    
    return NULL;
  }

  void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) { 
    uint32_t indegree, outdegree;
    for(auto it = values.begin(); it != values.end(); ++it) {
      uint32_t outdeg = (uint32_t) (*it);
      uint32_t indeg = (uint32_t) ((*it) >> 32);

      outdegree += outdeg;
      indegree += indeg;
    }

    return NULL; 
  }
};

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  assert(to.size() == 1);
  assert(from.size() == 1);
  
  to[0] += from[0];
  
  return NULL;
}

int main(int argc, char** argv) {
  DegreeCount<uint32_t, uint64_t> dc;
  if (argc != 6) {
    std::cout << "Usage: " << argv[0] << " <folderpath> <nmappers> <nreducers> <batchsize> <kitems>" << std::endl;
    return 0;
  }

  std::string folderpath = argv[1];
  int nmappers = atoi(argv[2]);
  int nreducers = atoi(argv[3]);
  int batchSize = atoi(argv[4]);
  int kitems = atoi(argv[5]);

  assert(batchSize > 0);
  dc.init(folderpath, nmappers, nreducers, batchSize, kitems);
  dc.run(); 
  
  return 0;
}

