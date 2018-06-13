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

#ifndef __HM_UTIL_H_
#define __HM_UTIL_H_

#include <limits>
#include <iomanip>
#include <sstream>
#include <string>

#include <google/protobuf/stubs/common.h>
typedef ::google::protobuf::uint64 IdType;

#define MAX_ID_VALUE (ULONG_MAX)

unsigned numDigits(std::streamoff num) {
 int ndigits = 0;
  while (num) {
    num /= 10;
    ndigits++;
  }
  return ndigits;
}

#define MAX_CHARS_STREAMOFF ( numDigits(std::numeric_limits<std::streamoff>::max()) )

#endif //__HM_UTIL_H_

