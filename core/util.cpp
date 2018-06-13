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

#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <cassert>
#include <stdlib.h>
#include <ctype.h>
#include <dirent.h>

double getTimer() {
  struct timeval t;
  gettimeofday(&t, NULL);
  return t.tv_sec*1000 + t.tv_usec/1000.0;
}

void getListOfFiles(std::string directory, std::vector<std::string>* fileList) {
  DIR *dp;
  struct dirent *dirp;

  if ((dp = opendir(directory.c_str())) == NULL)
    return;

  //Also check . and .. are not included in the filename
  std::string forbiddenFilename1 = ".";
  std::string forbiddenFilename2 = "..";
  while ((dirp = readdir(dp)) != NULL) {
    std::string fname = std::string(dirp->d_name);
    if (fname.compare(forbiddenFilename1) != 0 && fname.compare(forbiddenFilename2) != 0)
      fileList->push_back(fname);
  }
  closedir(dp);
}

unsigned stringHash(const std::string& str) {
  unsigned ret = 0;
  for(unsigned i=0; i<str.size(); ++i)
    ret += str[i];

  return ret;
}
