// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "btree_map.h"

using namespace std;
using namespace btree;

#define NB_INSERTS 10000LU

static unsigned long x=123456789, y=362436069, z=521288629;
unsigned long xorshf96(void) {          //period 2^96-1
   unsigned long t;
   x ^= x << 16;
   x ^= x >> 5;
   x ^= x << 1;

   t = x;
   x = y;
   y = z;
   z = t ^ x ^ y;

   return z;
}

int main(int argc, char**argv) {
   btree_map<uint64_t, int> *b = new btree_map<uint64_t, int>();

   for(size_t i = 0; i < NB_INSERTS; i++) {
      uint64_t hash = xorshf96()%NB_INSERTS;
      b->insert(make_pair(hash, 1));
   }
   printf("Size %lu\n", b->size());

   return 0;
}
