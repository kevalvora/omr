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

#include "fileIO.h"
#include "util.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_DATA_LENGTH (sizeof(T))

#define OFFSET_ADJUST(tid) (0)

template<typename T>
OnePhaseFileIO<T>::OnePhaseFileIO(const char *loc, unsigned threadCount)
  : FileIO<T>(loc, threadCount) {
  dataFileMutex = NULL; 
  data_iofs = NULL;

  fprintf(stderr, "MAX_DATA_LENGTH = %ld\n", MAX_DATA_LENGTH);
  init();
}

template<typename T>
OnePhaseFileIO<T>::~OnePhaseFileIO() {
  shutdown();
}

template<typename T>
void OnePhaseFileIO<T>::shutdown() {
  char path[256];
  strcat(strcpy(path, "rm -rf "), this->tmpFilePath());
  system(path); 
}

template<typename T>
void OnePhaseFileIO<T>::init() {
  char tpath[256];
  strcat(strcpy(tpath, "rm -rf "), this->tmpFilePath()); 
  int t = system(tpath); assert( t == 0 );

  assert(std::numeric_limits<long>::max() == std::numeric_limits<std::streamoff>::max());

  strcat(strcpy(tpath, "mkdir -p "), this->tmpFilePath());
  strcat(tpath, "/data");
  t = system(tpath);

  null_string = (char *) calloc(MAX_DATA_LENGTH, sizeof(char)); 

  data_iofs = (int *)calloc(this->nthreads, sizeof(int));
  dataFileMutex = (pthread_mutex_t **)calloc(this->nthreads, sizeof(pthread_mutex_t *));
  assert((dataFileMutex != NULL) && (data_iofs != NULL));

  for(unsigned i=0; i<this->nthreads; i++) {
    dataFileMutex[i] = (pthread_mutex_t *)calloc(PARALLEL_ACCESS, sizeof(pthread_mutex_t));
    assert((dataFileMutex[i] != NULL));

    char path[256]; char suffix[256];
    sprintf(suffix, "/data/data_%d", i); 
    data_iofs[i] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_CREAT | O_LARGEFILE | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    assert(data_iofs[i] > 0);

    for(unsigned j=0; j<PARALLEL_ACCESS; j++) {
      pthread_mutex_init(&dataFileMutex[i][j], NULL);
    }
  }
}

template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  unsigned hid = this->hash(startKey);
  pthread_mutex_lock( &dataFileMutex[tid][hid] ); 
  int err = pwrite(data_iofs[tid], arrayOfItems, MAX_DATA_LENGTH*nItems, (startKey-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == (int)(nItems*MAX_DATA_LENGTH)); //problematic cast?
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );
  return FILEIO_SUCCESS;
}

template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  unsigned hid = this->hash(startKey);
  pthread_mutex_lock( &dataFileMutex[tid][hid] ); 
  int err = pread(data_iofs[tid], arrayOfItems, nItems*MAX_DATA_LENGTH, (startKey-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == (int)(nItems*MAX_DATA_LENGTH));
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );
  return FILEIO_SUCCESS;
}

