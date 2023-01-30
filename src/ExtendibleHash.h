/*
Copyright 2023, The DyTIS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#pragma once

typedef struct ExtendibleHash ExtendibleHash;

struct ExtendibleHash {
  Directory** seg;

  ExtendibleHash(void) {
    seg = NULL;
  }

  ExtendibleHash(short GD) {
    uint64_t capacity = (pow(2, GD));
    seg = new Directory*[capacity];
  }

  ~ExtendibleHash(void) {
    delete [] seg;
  }

  size_t data_size(void) {
    size_t size = sizeof(ExtendibleHash);
    return size;
  }

  inline int Insert(Key_t&, Value_t, short);
  inline bool Delete(Key_t&, short);
  inline Value_t Get(Key_t&, short);
  inline void Scan(Key_t&, int&, size_t, Value_t*, short);
  inline Value_t* Find(Key_t&, short);

};


