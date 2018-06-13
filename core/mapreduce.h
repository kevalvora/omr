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

#include "util.h"
#include "infinimem/fileIO.h"
#include "mapwriter.h"

template <typename KeyType, typename ValueType>
void* doMap(void* arg);

template <typename KeyType, typename ValueType>
void* doReduce(void* arg);

template <typename KeyType, typename ValueType>
void* doInMemoryReduce(void* arg);

template <typename KeyType, typename ValueType>
class MapReduce
{
  public:
    virtual void* beforeMap(const unsigned tid) { return NULL; };
    virtual void* map(const unsigned tid, const unsigned fileId, const std::string& input) = 0;
    virtual void* afterMap(const unsigned tid) { return NULL; };
    virtual void* beforeReduce(const unsigned tid) { return NULL; };
    virtual void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) = 0; 
    virtual void* afterReduce(const unsigned tid) { return NULL; };
     
    virtual void run();
    void init(const std::string input, const unsigned mappers, const unsigned reducers, const unsigned bSize, const unsigned kItems);
  
    void writeBuf(const unsigned tid, const KeyType& key, const ValueType& value); 
    bool read(const unsigned tid);
    void readInit(const unsigned buffer);

    unsigned nMappers;
    unsigned nReducers;
    unsigned batchSize;  
    unsigned kBItems;  

    std::vector<double> map_times;
    std::vector<double> reduce_times;

    std::vector<std::string> fileList;

    pthread_barrier_t barMap;
    pthread_barrier_t barReduce;

    friend void* doMap<KeyType, ValueType>(void* arg);
    friend void* doReduce<KeyType, ValueType>(void* arg);
    friend void* doInMemoryReduce<KeyType, ValueType>(void* arg);

  private:
    std::string inputFolder;
    MapWriter<KeyType, ValueType> writer;
};

