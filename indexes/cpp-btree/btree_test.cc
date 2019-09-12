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
#include "../../utils.h"
#include "../rbtree.h"

using namespace std;
using namespace btree;

#define NB_INSERTS 10000000LU

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
   declare_timer;
   btree_map<uint64_t, struct rbtree_entry> *b = new btree_map<uint64_t, struct rbtree_entry>();

   start_timer {
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         struct rbtree_entry e;
         b->insert(make_pair(hash, e));
      }
   } stop_timer("BTREE - Time for %lu inserts/replace (%lu inserts/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);

   start_timer {
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         b->find(hash);
      }
   } stop_timer("BTREE - Time for %lu finds (%lu finds/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);

   return 0;
}
