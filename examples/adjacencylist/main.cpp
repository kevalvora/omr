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

#define USE_NUMERICAL_HASH

#include "data.pb.h"
#include "../../core/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include <limits>

const uint32_t null = std::numeric_limits<uint32_t>::max();

// Two uint32_t packed in one uint64_t such that incoming vertex id is on MSBs and outgoing vertex id is on LSBs
// numeric_limits<uint32_t>::max() is a special value which means no vertex
template <typename KeyType, typename ValueType>
class AdjacencyList : public MapReduce<KeyType, ValueType> {
  public:
  void* map(const unsigned tid, const unsigned fileId, const std::string& input) {
    if(input[0] == '%' || input[0] == '#')
      return NULL;

    std::stringstream inputStream(input);
    uint32_t src, dst;
    inputStream >> src >> dst;

      uint64_t outgoing = (static_cast<uint64_t>(null) << 32) | dst;
      this->writeBuf(tid, src, outgoing);

      uint64_t incoming = (static_cast<uint64_t>(src) << 32) | null;
      this->writeBuf(tid, dst, incoming);

    return NULL;
  }

  void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) { 
    std::vector<uint32_t> incomings, outgoings;
    for(auto it = values.begin(); it != values.end(); ++it) {
      uint32_t outgoing = (uint32_t) (*it);
      uint32_t incoming = (uint32_t) ((*it) >> 32);

      if(outgoing != null) outgoings.push_back(outgoing);
      if(incoming != null) incomings.push_back(incoming);
    }

    return NULL; 
  }
};

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  to.insert(to.end(), from.begin(), from.end());
  return NULL;
}

int main(int argc, char** argv) {
  AdjacencyList<uint32_t, uint64_t> al;
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

  al.init(folderpath, nmappers, nreducers, batchSize, kitems);
  al.run(); 

  return 0;
}

