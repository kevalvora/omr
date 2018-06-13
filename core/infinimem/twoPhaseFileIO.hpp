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
#include <iostream>

#include <stdio.h>
#include <stdio_ext.h>

#include <errno.h>

#define META_LINE_LENGTH (2*(MAX_CHARS_STREAMOFF + 1))

  template<typename T>
TwoPhaseFileIO<T>::TwoPhaseFileIO(const char *loc, unsigned threadCount)
  : FileIO<T>(loc, threadCount) {
    data_ifs = NULL;
    meta_iofs = NULL;
    data_ofs = NULL;

    init();
  }

template<typename T>
TwoPhaseFileIO<T>::~TwoPhaseFileIO() {
  shutdown();
}

template<typename T>
void TwoPhaseFileIO<T>::shutdown() {
  char path[256];
  strcat(strcpy(path, "rm -rf "), this->tmpFilePath());
  system(path);
}

template<typename T>
void TwoPhaseFileIO<T>::init() {
  char tpath[256];
  strcat(strcpy(tpath, "rm -rf "), this->tmpFilePath()); 
  int t = system(tpath); assert( t == 0 );

  strcat(strcpy(tpath, "mkdir -p "), this->tmpFilePath());

  char dmpath[256]; 
  strcpy(dmpath, tpath);
  strcat(dmpath, "/data");
  t = system(dmpath);

  strcpy(dmpath, tpath);
  strcat(dmpath, "/meta");
  t = system(dmpath); 

  null_string = (char *) calloc(META_LINE_LENGTH, sizeof(char)); 

  meta_iofs = (int **)calloc(this->nthreads, sizeof(int *));
  metaFileMutex = (pthread_mutex_t **)calloc(this->nthreads, sizeof(pthread_mutex_t *));

  data_ifs = (int **)calloc(this->nthreads, sizeof(int *));
  data_ofs = (int *)calloc(this->nthreads, sizeof(int));
  dataFileMutex = (pthread_mutex_t *)calloc(this->nthreads, sizeof(pthread_mutex_t));

  assert((data_ofs != NULL) && (meta_iofs != NULL) && (data_ifs != NULL) 
      && (dataFileMutex != NULL) && (metaFileMutex != NULL));

  for(unsigned i=0; i<this->nthreads; i++) {
    char suffix[256]; char path[256];
    sprintf(suffix, "/data/data_%d", i);
    data_ofs[i] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_CREAT | O_LARGEFILE | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    pthread_mutex_init(&dataFileMutex[i], NULL);
    assert(data_ofs[i] > 0);

    metaFileMutex[i] = (pthread_mutex_t *)calloc(PARALLEL_ACCESS, sizeof(pthread_mutex_t));
    meta_iofs[i] = (int *)calloc(PARALLEL_ACCESS, sizeof(int));
    data_ifs[i] = (int *)calloc(PARALLEL_ACCESS, sizeof(int));
    assert((data_ofs[i]) && (meta_iofs[i] != NULL) && (data_ifs[i] != NULL) && (metaFileMutex[i] != NULL));

    for(int j=0; j<PARALLEL_ACCESS; j++) {
      pthread_mutex_init(&metaFileMutex[i][j], NULL);
      sprintf(suffix, "/data/data_%d", i);
      data_ifs[i][j] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_LARGEFILE | O_RDONLY);
      assert(data_ifs[i][j] > 0);

      sprintf(suffix, "/meta/meta_%d", i); 
      meta_iofs[i][j] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_CREAT | O_LARGEFILE | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
      assert(meta_iofs[i][j] > 0);
    }
  }
}

template<typename T>
fileIoReturn_t TwoPhaseFileIO<T>::file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  unsigned hid = this->hash(startKey);

  char **meta = (char **)calloc(nItems, sizeof(char *));
  for(unsigned i=0; i<nItems; i++) {
    meta[i] = (char *)calloc(1, META_LINE_LENGTH);
    assert(meta[i] != 0);
  }
  pthread_mutex_lock( &dataFileMutex[tid] );
  off64_t datastart = lseek(data_ofs[tid], 0, SEEK_END); assert(datastart != -1);
  for(unsigned i=startKey; i<startKey+nItems; i++) {
    bool good = arrayOfItems[i-startKey].SerializeToFileDescriptor(data_ofs[tid]); assert(good == true);
    sprintf(meta[i-startKey], "%019d %019ld", arrayOfItems[i-startKey].ByteSize(), datastart); meta[i-startKey][META_LINE_LENGTH] = '\0';
    datastart = lseek(data_ofs[tid], 0, SEEK_CUR); assert(datastart != -1);
  }
  pthread_mutex_unlock( &dataFileMutex[tid] );

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  int err = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * startKey, SEEK_SET); assert(err != -1);
  for(unsigned i=startKey; i<startKey+nItems; i++) {
    err = write(meta_iofs[tid][hid], meta[i-startKey], META_LINE_LENGTH); assert(err != -1);
  }
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );

  for(unsigned i=0; i<nItems; i++)
    free(meta[i]);
  free(meta);

  return FILEIO_SUCCESS;
}

template<typename T>
fileIoReturn_t TwoPhaseFileIO<T>::file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  unsigned hid = this->hash(startKey);

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  off64_t metastart = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * startKey, SEEK_SET); assert(metastart != -1);
  char length_str[META_LINE_LENGTH], *datapointer_str;
  int err = read(meta_iofs[tid][hid], length_str, META_LINE_LENGTH);
  assert(err != -1);
  datapointer_str = length_str + MAX_CHARS_STREAMOFF + 1;
  length_str[MAX_CHARS_STREAMOFF] = datapointer_str[MAX_CHARS_STREAMOFF] = '\0';
  off64_t datapointer = atoll(datapointer_str);
  assert(datapointer != -1);

  std::streamoff* lengths = (std::streamoff *)calloc(nItems, sizeof(std::streamoff)); assert(lengths !=  NULL); //gk
  lengths[0] = atol(length_str);
  for(unsigned i=startKey+1; i<startKey+nItems; i++) {
    err = read(meta_iofs[tid][hid], length_str, META_LINE_LENGTH);
    assert(err != -1);
    length_str[MAX_CHARS_STREAMOFF] = datapointer_str[MAX_CHARS_STREAMOFF] = '\0';
    lengths[i-startKey] = atol(length_str);
  }
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );

  pthread_mutex_lock( &dataFileMutex[tid] );
  err = lseek(data_ifs[tid][hid], datapointer, SEEK_SET); assert(err != -1);
  for(unsigned i=startKey; i<startKey+nItems; i++) {
    void *buf = malloc(lengths[i-startKey]); assert(buf != NULL);
    err = read(data_ifs[tid][hid], buf, lengths[i-startKey]);
    assert(err != -1);
    bool good = arrayOfItems[i-startKey].ParseFromArray(buf, lengths[i-startKey]); 
    assert(good = true);
    free(buf);
  }
  pthread_mutex_unlock( &dataFileMutex[tid] );
  free(lengths);

  return FILEIO_SUCCESS;
}

