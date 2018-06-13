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

#ifndef __RECORD_TYPE__
#define __RECORD_TYPE__

#include <string>
#include <cstring>
#include <cassert>

struct RecordType {
  public:

    const std::string key() const { 
      return _key;
    }

    void set_key(const std::string& k) {
      assert(k.size() <= MAX_WORD_SIZE);
      std::copy(k.begin(), k.end(), _key);
      _key[k.size()] = '\0';
    } 
    
    const std::string value() const {
      return _value;
    }

    void set_value(const std::string& v) {
      assert(v.size() == MAX_NUM_FILES);
      std::copy(v.begin(), v.end(), _value);
      _value[MAX_NUM_FILES] = '\0';
    }

  private:
    char _key[MAX_WORD_SIZE + 1];
    char _value[MAX_NUM_FILES + 1];
};

#endif
