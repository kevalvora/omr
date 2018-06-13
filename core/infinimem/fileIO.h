/*
MIT License

Copyright (c) 2018 Sai Charan Koduru <scharan@cs.ucr.edu>

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

#ifndef _FILE_IO_H__
#define _FILE_IO_H__

#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <assert.h>
#include <typeinfo> 
#include "util.h"
#include <unistd.h>
#include <iostream>

#include <vector>

#define _FILE_OFFSET_BITS 64

#define PARALLEL_ACCESS 64
#define FILEIO_MAX_KEY 256

typedef int fileIoReturn_t;
typedef fileIoReturn_t fileIoReturn;

enum fileIoReturntypes {
  FILEIO_ERROR = -1, FILEIO_SUCCESS = 0, FILEIO_NOTSTORED = 1, FILEIO_DATA_EXISTS = 2
};

template<typename T>
class FileIO {
  private:
    std::string tmpFileLocation;

  public:
    unsigned nthreads;

    FileIO(const char *filePath, unsigned threadCount) {
      nthreads = threadCount;
      tmpFileLocation = filePath;
    }

    virtual ~FileIO() { }

    inline void clearStats() { }

    inline int hash(IdType id) const {
      return id % PARALLEL_ACCESS;
    }
    inline const char *tmpFilePath() const {
      return tmpFileLocation.c_str();
    }

    virtual void init() = 0;
    virtual void shutdown() = 0;

    virtual fileIoReturn_t file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems,
        T* arrayOfItems) = 0;
    virtual fileIoReturn_t file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems,
        T* arrayOfItems) = 0;
};

template<typename T>
class OnePhaseFileIO: public FileIO<T> {
  public:
    OnePhaseFileIO(const char *loch, unsigned threadCount);
    ~OnePhaseFileIO();

    void init();
    void shutdown();

    fileIoReturn_t file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);
    fileIoReturn_t file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);

  private:
    pthread_mutex_t **dataFileMutex;
    int *data_iofs;
    char *null_string;
};

template<typename T>
class TwoPhaseFileIO: public FileIO<T> {
  public:
    TwoPhaseFileIO(const char *loch, unsigned threadCount);
    ~TwoPhaseFileIO();

    void init();
    void shutdown();

    fileIoReturn_t file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);
    fileIoReturn_t file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);

  private:
    int **data_ifs;
    int **meta_iofs;
    int *data_ofs;
    char *null_string;

    pthread_mutex_t *dataFileMutex; 
    pthread_mutex_t **metaFileMutex; 
};

#endif // _FILE_IO_H__

