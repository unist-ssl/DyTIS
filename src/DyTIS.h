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
#include <stdio.h>

#include <cstring>
#include <thread>
#include <mutex>
#include <cmath>
#include <stdlib.h>
#include "util/util.h"
#include "src/Directory.h"
#include "src/ExtendibleHash.h"
#include "util/pair.h"


const size_t kCapacity = (1 << kDepth);
typedef class DyTIS DyTIS;

class DyTIS {
  private:
    ExtendibleHash** EH;

  public:
  DyTIS(void);
  ~DyTIS(void);
  inline void Insert(Key_t&, Value_t);
  inline bool Delete(Key_t&);
  inline Value_t Get(Key_t&);
  inline Value_t* Scan(Key_t&, size_t);
  inline Value_t* Find(Key_t&);
  inline bool Update(Key_t&, Value_t);

};

