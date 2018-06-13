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

#define MAX_WORD_SIZE 32
#define MAX_NUM_FILES 32

#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#endif

#define USE_STRING_HASH

#include "../../core/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>

static std::string outputPrefix = "";
static unsigned noOfFiles = MAX_NUM_FILES;

template <typename KeyType, typename ValueType>
class InvertedIndex : public MapReduce<KeyType, ValueType>
{
  static thread_local std::string bitset;
  public:
  void* beforeMap(const unsigned tid) {
    bitset = "";
    for(unsigned i=0; i<noOfFiles; ++i)
      bitset += "0";

    return NULL;
  }

  void* map(const unsigned tid, const unsigned fileId, const std::string& input)
  {
    std::stringstream inputStream(input);
    std::string token;
    unsigned fid = fileId % noOfFiles; 

    bitset[fid] = '1';
    while (inputStream >> token) {
#ifdef USE_ONE_PHASE_IO
      unsigned idx = 0;
      while(idx < token.size()) {
        std::string miniToken = token.substr(idx, MAX_WORD_SIZE);
        this->writeBuf(tid, miniToken, bitset);
        idx += (MAX_WORD_SIZE);
      } 
#else
      this->writeBuf(tid, token, bitset);
#endif
    }

    bitset[fid] = '0';
    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
    bitset = "";
    for(unsigned i=0; i<noOfFiles; ++i)
      bitset += "0";

    return NULL;
  }

  void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) { 
      if(bitset.size() != noOfFiles)
        std::cout << "key = " << key << " bitset.size() = " << bitset.size() << " noOfFiles = " << noOfFiles << std::endl;
      assert(bitset.size() == noOfFiles);
      for(unsigned i=0; i<noOfFiles; ++i)
        bitset[i] = '0';

      for (auto it = values.begin(); it != values.end(); ++it) {
        assert(it->size() == noOfFiles);
        for(unsigned j=0; j<noOfFiles; ++j)
          if((*it)[j] == '1')
            bitset[j] = '1';
      }

    return NULL;
  }
};

template <typename KeyType, typename ValueType>
thread_local std::string InvertedIndex<KeyType, ValueType>::bitset;

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  assert(to.size() == 1);
  assert(from.size() == 1);

  for(unsigned i=0; i<noOfFiles; ++i)
    if(from[0][i] == '1')
      to[0][i] = '1';

  return NULL;
}

int main(int argc, char** argv) {
  InvertedIndex<std::string, std::string> ii;
  if (argc != 6)
  {
    std::cout << "Usage: " << argv[0] << " <folderpath> <nmappers> <nreducers> <batchsize> <kitems>" << std::endl;
    return 0;
  }

  std::string folderpath = argv[1];
  int nmappers = atoi(argv[2]);
  int nreducers = atoi(argv[3]);
  int batchSize = atoi(argv[4]);
  int kitems = atoi(argv[5]);

  assert(batchSize > 0);

  noOfFiles = MAX_NUM_FILES;

  ii.init(folderpath, nmappers, nreducers, batchSize, kitems);
  ii.run(); 

  return 0;
}

